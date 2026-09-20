# Escalado del SSE Gateway en AWS con Application Load Balancer

SSE en un load balancer tiene trampas no obvias que pueden hacer que el sistema funcione en local pero falle silenciosamente en producción. Este documento cubre cada una.

---

## El problema central

SSE es una conexión HTTP de larga duración. El cliente abre un GET y la mantiene abierta minutos u horas recibiendo datos. Los load balancers están diseñados para peticiones cortas y tienen comportamientos por defecto que cortan conexiones "inactivas" — pero una conexión SSE entre eventos no tiene tráfico de datos y parece inactiva al balanceador.

El resultado sin configuración adecuada: las conexiones SSE se cortan cada 60 segundos (el timeout por defecto del ALB) y el cliente reconecta continuamente, generando carga extra y degradando la experiencia.

---

## Arquitectura recomendada

```
                              ┌──────────────────────────────────────────┐
                              │                  AWS                      │
                              │                                           │
Clientes ──── HTTPS ────────▶│  CloudFront*          ┌──────────────┐   │
                              │  (solo assets         │     ALB      │   │
                              │   estáticos)          │  :443 HTTPS  │   │
                              │                       └──────┬───────┘   │
                              │                              │            │
                              │                    Target Group           │
                              │                    /subscribe             │
                              │                              │            │
                              │            ┌─────────────────────────┐   │
                              │            │  ECS Fargate / EC2 ASG  │   │
                              │            │                         │   │
                              │            │  ┌──────┐  ┌──────┐    │   │
                              │            │  │ GW-1 │  │ GW-2 │ …  │   │
                              │            │  └──┬───┘  └──┬───┘    │   │
                              │            └─────┼─────────┼────────┘   │
                              │                  │         │             │
                              │            ┌─────▼─────────▼──────┐     │
                              │            │    NATS Cluster       │     │
                              │            │  (EC2 / ECS / Synadia)│     │
                              │            └──────────────────────┘     │
                              └──────────────────────────────────────────┘

* CloudFront NO debe estar frente a /subscribe — ver sección CloudFront.
```

> **La ventaja clave de este sistema**: gracias a NATS, **no se necesitan sticky sessions**. Cualquier instancia del gateway puede atender a cualquier cliente. Cuando se publica un evento en NATS, todas las instancias lo reciben y lo reenvían a sus clientes conectados.

---

## 1. Configuración del ALB

### 1.1 Idle Timeout — el parámetro más importante

El ALB cierra conexiones que llevan más de `idle_timeout_seconds` sin actividad TCP.

Arecibo-SSE envía un comentario SSE de keepalive cada **25 segundos**. Ese tráfico reinicia el contador de inactividad del ALB. Por lo tanto, el idle timeout del ALB debe ser **mayor que 25 segundos**.

**Recomendación: configurar a 3600 segundos (1 hora).**

> Por qué 3600 y no, por ejemplo, 60: si un usuario tiene la pestaña abierta viendo un dashboard durante 40 minutos, queremos que su conexión siga viva sin reconexiones. El keepalive de 25s garantiza que el ALB no la corte. 3600s es un límite superior cómodo para sesiones humanas.

```bash
# AWS CLI
aws elbv2 modify-load-balancer-attributes \
  --load-balancer-arn <ARN_DEL_ALB> \
  --attributes Key=idle_timeout.timeout_seconds,Value=3600
```

```hcl
# Terraform
resource "aws_lb" "gateway" {
  name               = "arecibo-gateway-alb"
  internal           = false
  load_balancer_type = "application"
  subnets            = var.public_subnet_ids
  security_groups    = [aws_security_group.alb.id]

  # El parámetro crítico para SSE.
  idle_timeout = 3600
}
```

### 1.2 Target Group

El target group necesita tres ajustes no obvios:

**a) Deregistration delay** — cuando el auto-scaling elimina una instancia, el ALB deja de enviarle tráfico nuevo pero mantiene las conexiones existentes durante este período. Si es muy corto, los clientes SSE activos se cortan abruptamente. `EventSource` reconectará automáticamente, pero la experiencia es mejor si damos tiempo.

**Recomendación: 300 segundos (5 minutos).**

**b) Protocol version** — usar HTTP/1.1 entre ALB e instancia, no HTTP/2. HTTP/2 tiene multiplexing pero Go's `net/http` maneja esto internamente; forzar HTTP/2 en el target group puede causar problemas de buffering con SSE en algunos entornos.

**c) Slow start** — no activar. Slow start limita el tráfico a instancias nuevas durante el período de calentamiento, pero las conexiones SSE son de larga duración y slow start las bloquearía innecesariamente.

```hcl
resource "aws_lb_target_group" "gateway" {
  name        = "arecibo-gateway-tg"
  port        = 8081
  protocol    = "HTTP"
  vpc_id      = var.vpc_id
  target_type = "ip"  # necesario para ECS Fargate

  # Tiempo para que los clientes SSE activos reciban el mensaje de cierre
  # antes de que la instancia sea terminada.
  deregistration_delay = 300

  # Sin slow start — los clientes SSE son de larga duración,
  # no necesitamos "calentar" la instancia gradualmente.
  slow_start = 0

  health_check {
    path                = "/health"
    protocol            = "HTTP"
    healthy_threshold   = 2
    unhealthy_threshold = 3
    timeout             = 5
    interval            = 15
    matcher             = "200"
  }

  # Sin stickiness — NATS hace el fan-out a todas las instancias.
  # Sticky sessions en SSE son un antipatrón con brokers de mensajes.
  stickiness {
    enabled = false
    type    = "lb_cookie"
  }
}
```

> **Por qué NO usar sticky sessions**: sin NATS, una arquitectura SSE necesita sticky sessions porque solo el servidor con la conexión abierta puede enviar al cliente. Con NATS, todas las instancias del gateway están suscritas al mismo topic y todas reciben el evento simultáneamente. El fan-out lo hace NATS, no el load balancer. Sticky sessions añaden estado al ALB y complican el escalado.

### 1.3 Listener HTTPS

```hcl
resource "aws_lb_listener" "https" {
  load_balancer_arn = aws_lb.gateway.arn
  port              = 443
  protocol          = "HTTPS"
  ssl_policy        = "ELBSecurityPolicy-TLS13-1-2-2021-06"
  certificate_arn   = var.acm_certificate_arn

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.gateway.arn
  }
}

# Redirección HTTP → HTTPS
resource "aws_lb_listener" "http_redirect" {
  load_balancer_arn = aws_lb.gateway.arn
  port              = 80
  protocol          = "HTTP"

  default_action {
    type = "redirect"
    redirect {
      port        = "443"
      protocol    = "HTTPS"
      status_code = "HTTP_301"
    }
  }
}
```

---

## 2. Por qué no necesitas sticky sessions

Esta es la decisión arquitectónica más importante del sistema y vale la pena documentarla con detalle.

### Sin NATS (arquitectura naïve)

```
Cliente A ──── SSE ────▶ Gateway-1 (tiene la conexión)
Publisher ── POST ─────▶ ALB
                          ├──▶ Gateway-1 ✅ (tiene conexión de A, puede enviar)
                          └──▶ Gateway-2 ❌ (no tiene conexión de A, no puede enviar)
```

Si el POST llega al Gateway-2, el cliente A no recibe el evento. Para evitarlo: sticky sessions que garanticen que el POST siempre llega al mismo servidor. Esto funciona pero impide el escalado real.

### Con NATS (Arecibo-SSE)

```
Cliente A ──── SSE ────▶ Gateway-1 ─── suscribe(topic) ──▶ NATS
Cliente B ──── SSE ────▶ Gateway-2 ─── suscribe(topic) ──▶ NATS

Publisher ── POST ─────▶ ALB
                          └──▶ Gateway-N ── publish(topic) ──▶ NATS
                                                                 ├──▶ Gateway-1 ──▶ Cliente A ✅
                                                                 └──▶ Gateway-2 ──▶ Cliente B ✅
```

El publisher puede llegar a cualquier instancia. NATS distribuye el evento a todas las instancias suscritas. Cada instancia reenvía a sus clientes SSE. No hay estado en el load balancer.

**Consecuencias prácticas:**
- Auto-scaling sin fricción: añadir una instancia solo requiere que se conecte a NATS
- Eliminación de instancias sin pérdida de eventos: los eventos se reenvían a otras instancias
- Sin cambios de configuración del ALB al escalar

---

## 3. Auto-scaling del gateway

### 3.1 Métricas para escalar

Las métricas estándar (CPU, memoria) no son las mejores para un servicio SSE. El gateway es IO-bound, no CPU-bound. Una instancia puede manejar miles de conexiones SSE con CPU casi en 0.

**Mejor métrica: número de conexiones activas.**

Publica una métrica personalizada a CloudWatch desde el gateway:

```go
// Puedes agregar esto al gateway para trackear conexiones activas.
// Llama a activeConns.Add(1) al conectar y activeConns.Add(-1) al desconectar.
var activeConns atomic.Int64

// Publicar a CloudWatch cada 30s desde un goroutine en background.
// Requiere github.com/aws/aws-sdk-go-v2 — agregar si se implementa esta métrica.
```

O usa una métrica proxy más simple: **ALBRequestCountPerTarget**. Cuando sube, hay más conexiones SSE entrantes.

```hcl
resource "aws_appautoscaling_policy" "gateway_scale_out" {
  name               = "gateway-scale-out"
  service_namespace  = "ecs"
  resource_id        = "service/${var.cluster_name}/${var.service_name}"
  scalable_dimension = "ecs:service:DesiredCount"
  policy_type        = "TargetTrackingScaling"

  target_tracking_scaling_policy_configuration {
    target_value = 1000  # conexiones por instancia

    customized_metric_specification {
      metric_name = "ActiveConnectionCount"
      namespace   = "AreciboSSE/Gateway"
      statistic   = "Sum"
    }

    scale_in_cooldown  = 300  # esperar 5 min antes de reducir
    scale_out_cooldown = 60   # escalar rápido hacia arriba
  }
}
```

### 3.2 Scale-in graceful — el problema real

Al reducir instancias, el flujo es:

```
1. ASG / ECS marca la instancia para terminación
2. ALB mueve la instancia a "draining" — deja de enviar tráfico nuevo
3. ALB espera deregistration_delay (300s en nuestra config)
4. Durante ese tiempo, los clientes SSE activos siguen conectados
5. El gateway recibe SIGTERM → inicia shutdown con 30s de gracia
6. Los clientes SSE detectan que la conexión se cerró
7. EventSource reconecta automáticamente a otra instancia (2-3 segundos)
8. La nueva instancia se suscribe a NATS y el cliente empieza a recibir eventos
```

> **Problema potencial**: si el deregistration_delay (300s) es mayor que el shutdown graceful del gateway (30s), el gateway termina antes de que el ALB deje de enviarle tráfico. Las conexiones en "draining" se cortan abruptamente.

**Solución**: alargar el shutdown graceful del gateway a 310 segundos en producción, o reducir el deregistration_delay a 25 segundos si las reconexiones rápidas son aceptables.

```bash
# Variable de entorno para controlar el graceful shutdown en producción
GATEWAY_SHUTDOWN_TIMEOUT=310s
```

Actualiza `services/gateway/cmd/main.go` para leer este valor:

```go
// Leer el timeout de shutdown del entorno para poder ajustarlo por ambiente
// sin recompilar. En dev son 30s, en prod con ALB son 310s.
shutdownTimeout := 30 * time.Second
if v := os.Getenv("GATEWAY_SHUTDOWN_TIMEOUT"); v != "" {
    if d, err := time.ParseDuration(v); err == nil {
        shutdownTimeout = d
    }
}
```

---

## 4. NATS en AWS

AWS no ofrece NATS como servicio gestionado (MSK es Kafka, no NATS). Las opciones son:

### Opción A: NATS en ECS Fargate (recomendado para empezar)

```hcl
resource "aws_ecs_task_definition" "nats" {
  family                   = "arecibo-nats"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "512"
  memory                   = "1024"

  container_definitions = jsonencode([{
    name  = "nats"
    image = "nats:2.10-alpine"
    command = [
      "--cluster_name", "arecibo",
      "--http_port", "8222",
      "--port", "4222",
    ]
    portMappings = [
      { containerPort = 4222, protocol = "tcp" },
      { containerPort = 8222, protocol = "tcp" },  # monitoreo
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = "/arecibo/nats"
        awslogs-region        = var.aws_region
        awslogs-stream-prefix = "nats"
      }
    }
  }])
}
```

**Limitación**: Fargate con una sola instancia NATS no tiene HA. Si el contenedor NATS cae, todos los clientes SSE pierden la suscripción. Para producción crítica, usar un cluster de 3 nodos.

### Opción B: NATS cluster en EC2 (HA real)

```
3 instancias EC2 en 3 AZs distintas, ejecutando nats-server con clustering.
Los gateways se conectan con URL de lista de todos los nodos:
NATS_URL=nats://nats-1:4222,nats://nats-2:4222,nats://nats-3:4222

El cliente nats.go ya maneja la reconexión automática al cluster.
```

El nats.go client ya está configurado con `MaxReconnects(-1)` — si un nodo cae, reconecta automáticamente al siguiente en la lista.

### Opción C: Synadia Cloud (NATS gestionado)

[Synadia Cloud](https://www.synadia.com) es el NATS as-a-Service de los creadores de NATS. Es la opción más simple para producción: sin gestión de infraestructura, HA incluida, con soporte para JetStream.

```bash
# NATS_URL con Synadia Cloud (formato NGS)
NATS_URL=tls://connect.ngs.global
```

---

## 5. CloudFront — compatibilidad y limitaciones

**CloudFront no funciona bien frente al endpoint SSE** por defecto. El problema es el buffering: CloudFront acumula la respuesta antes de enviarla al cliente, lo que destruye el tiempo real.

### Lo que NO debes hacer

```
❌ Clientes → CloudFront → ALB → Gateway /subscribe
   (CloudFront bufferizará el stream SSE)
```

### Configuración correcta

Excluye el path `/subscribe` de CloudFront completamente, apuntando ese tráfico directamente al ALB:

```hcl
resource "aws_cloudfront_distribution" "arecibo" {
  # Comportamiento por defecto: assets estáticos del frontend
  default_cache_behavior {
    target_origin_id = "frontend-s3"
    # ... configuración de caché normal
  }

  # Los assets del frontend (React build) van a S3/CloudFront normalmente.
  origin {
    origin_id   = "frontend-s3"
    domain_name = aws_s3_bucket.frontend.bucket_regional_domain_name
    # ...
  }

  # El gateway SSE va DIRECTO al ALB, sin pasar por CloudFront.
  # En el frontend, usar la URL del ALB directamente para /subscribe:
  # const es = new EventSource('https://sse.tudominio.com/subscribe?topic=...')
}
```

En el frontend, configura dos URLs separadas:

```typescript
// web/src/config.ts
export const GATEWAY_URL = import.meta.env.VITE_GATEWAY_URL ?? '';
// En producción:
// VITE_GATEWAY_URL=https://sse.tudominio.com  (apunta al ALB directamente)

// En dev, el proxy de Vite maneja /subscribe → localhost:8081 automáticamente.
const sseURL = GATEWAY_URL
  ? `${GATEWAY_URL}/subscribe?topic=${topic}`
  : `/subscribe?topic=${topic}`;
```

---

## 6. Security Groups

```hcl
# ALB: acepta tráfico de internet en 80 y 443
resource "aws_security_group" "alb" {
  name   = "arecibo-alb-sg"
  vpc_id = var.vpc_id

  ingress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# Gateway: solo acepta tráfico del ALB
resource "aws_security_group" "gateway" {
  name   = "arecibo-gateway-sg"
  vpc_id = var.vpc_id

  ingress {
    from_port       = 8081
    to_port         = 8081
    protocol        = "tcp"
    security_groups = [aws_security_group.alb.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# Publisher: solo acepta tráfico del ALB (si está expuesto) o de servicios internos
resource "aws_security_group" "publisher" {
  name   = "arecibo-publisher-sg"
  vpc_id = var.vpc_id

  ingress {
    from_port       = 8080
    to_port         = 8080
    protocol        = "tcp"
    security_groups = [aws_security_group.alb.id]
    # Si solo es llamado internamente, reemplazar por el SG del servicio que lo llama.
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# NATS: solo acepta tráfico de gateway y publisher
resource "aws_security_group" "nats" {
  name   = "arecibo-nats-sg"
  vpc_id = var.vpc_id

  ingress {
    from_port       = 4222
    to_port         = 4222
    protocol        = "tcp"
    security_groups = [
      aws_security_group.gateway.id,
      aws_security_group.publisher.id,
    ]
  }

  # Puerto de monitoreo: solo acceso interno (opcional, para dashboards)
  ingress {
    from_port   = 8222
    to_port     = 8222
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}
```

---

## 7. Checklist de producción

```
ALB
  [ ] idle_timeout = 3600 segundos
  [ ] Listener HTTPS con certificado ACM
  [ ] Redirección HTTP → HTTPS
  [ ] Access logs habilitados en S3

Target Group (Gateway)
  [ ] deregistration_delay = 300 segundos
  [ ] slow_start = 0
  [ ] stickiness.enabled = false
  [ ] health_check.path = /health
  [ ] health_check.interval = 15s

Gateway (ECS / EC2)
  [ ] GATEWAY_SHUTDOWN_TIMEOUT = 310s (mayor que deregistration_delay)
  [ ] ALLOWED_ORIGIN = dominio real (no "*")
  [ ] NATS_URL apunta al cluster de NATS (no a una sola instancia)
  [ ] Al menos 2 instancias (AZs distintas) para HA
  [ ] Auto-scaling configurado

NATS
  [ ] Al menos 3 nodos en 3 AZs para quorum
  [ ] TLS habilitado entre nodos y clientes
  [ ] NATS_URL en los servicios incluye todos los nodos de bootstrap

CloudFront
  [ ] /subscribe NO pasa por CloudFront
  [ ] VITE_GATEWAY_URL apunta al ALB directamente para SSE

Seguridad
  [ ] Gateway solo acepta tráfico del SG del ALB
  [ ] NATS solo acepta tráfico de gateway y publisher
  [ ] Publisher no expuesto a internet si es solo internal
```

---

## 8. Diagrama de reconexión en scale-in

Este es el flujo cuando el auto-scaling elimina una instancia con clientes activos:

```
t=0s   ASG decide terminar Gateway-2 (tiene 800 clientes SSE activos)
t=0s   ALB marca Gateway-2 como "draining" — sin tráfico nuevo
t=0s   Gateway-2 recibe SIGTERM — inicia shutdown de 310s
t=1s   Los 800 clientes de Gateway-2 siguen conectados
...
t=300s ALB expira deregistration_delay — corta conexiones de Gateway-2
t=300s Los 800 EventSource detectan cierre de conexión
t=302s EventSource reconecta automáticamente al ALB
t=302s ALB enruta las 800 reconexiones a Gateway-1 y Gateway-3
t=302s Gateway-1 y Gateway-3 suscriben los nuevos topics en NATS
t=303s Los 800 clientes están conectados de nuevo y reciben eventos
t=310s Gateway-2 termina — shutdown graceful completo
```

Desde el punto de vista del usuario: aproximadamente 2-3 segundos sin eventos, luego la conexión se restaura automáticamente sin intervención manual.
