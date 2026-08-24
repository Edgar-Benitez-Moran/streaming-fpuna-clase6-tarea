# Tarea 3  -  Estado, duplicados e idempotencia con Apache Beam

Asignatura: **Streaming de datos y sus aplicaciones**

Maestría en Análisis de Datos e Inteligencia Artificial

Facultad Politécnica - Universidad Nacional de Asunción

## Objetivo

Implementar un pipeline de pagos con Apache Beam capaz de producir totales `CONFIRMED` por comercio y por minuto, considerando correctamente:

* tiempo de evento (`event_time`);
* ventanas fijas de 60 segundos;
* eventos fuera de orden;
* hasta 120 segundos de lateness;
* deduplicación por `event_id` dentro de cada comercio;
* estado por clave con expiración mediante timer;
* panes acumulativos;
* reintentos;
* salida idempotente.

## Contrato temporal

El timestamp utilizado por el pipeline proviene de `event_time`, no del momento de llegada del evento.

Cada pago se asigna a una ventana fija de 60 segundos:

```text
[inicio, fin)
```

La política de streaming utiliza:

* `FixedWindows(60)`;
* `AfterWatermark`;
* emisión temprana mediante `AfterProcessingTime(10)`;
* revisiones tardías mediante `AfterCount(1)`;
* `allowed_lateness = 120` segundos;
* `AccumulationMode.ACCUMULATING`.

Un evento que llega después del cierre de su ventana puede producir una revisión tardía mientras permanezca dentro de la lateness permitida.

## Deduplicación, estado y expiración

La deduplicación se realiza por:

```text
merchant_id + event_id
```

El `merchant_id` constituye la clave de Beam y cada comercio mantiene su propio estado.

Los `event_id` observados se almacenan mediante `SetStateSpec`.

El estado no se conserva indefinidamente. Se programa un timer en tiempo de evento (`TimeDomain.WATERMARK`) para limpiar el estado al superar:

```text
fin de ventana + lateness permitida
```

Esto mantiene el estado acotado y permite que claves distintas permanezcan aisladas.

## Idempotencia y reintentos

La clave idempotente de un resultado lógico es:

```text
merchant_id|window_start
```

En modo idempotente se simula un `UPSERT`: múltiples reintentos de la misma entidad lógica reemplazan el mismo resultado y convergen a una única fila materializada.

En modo append-only se simula un `POST`: cada intento produce una nueva fila.

Por tanto, idempotencia y deduplicación cumplen funciones diferentes:

* la deduplicación evita volver a procesar el mismo evento;
* la idempotencia evita duplicar el efecto externo durante un reintento.

## Prueba temporal con TestStream

Además de las pruebas proporcionadas, se agregó:

```text
tests/test_temporal.py
```

La prueba utiliza `TestStream` para reproducir explícitamente:

1. un evento dentro de una ventana;
2. avance del watermark más allá del cierre;
3. llegada posterior de otro evento cuyo `event_time` todavía pertenece a la ventana;
4. aceptación del evento dentro de la lateness permitida;
5. generación de un pane `LATE` acumulativo.

Resultado observado:

```text
1 passed
```

sin warnings.

## Ejecución reproducible con Docker

### 1. Construir e iniciar el notebook

Desde la raíz del repositorio:

```bash
docker compose up --build -d notebook
```

### 2. Abrir Marimo

```text
http://localhost:2718
```

### 3. Ejecutar las pruebas

```bash
docker compose exec notebook uv run pytest
```

### 4. Ejecutar la prueba temporal adicional

```bash
docker compose exec notebook uv run pytest tests/test_temporal.py -v
```

### 5. Validar estilo

```bash
docker compose exec notebook uv run ruff check notebook.py tests/test_temporal.py
```

Resultado esperado:

```text
All checks passed!
```

### 6. Validar la estructura de Marimo

```bash
docker compose exec notebook uv run marimo check --strict notebook.py
```

La ejecución correcta finaliza sin errores.

## Estado de las pruebas provistas

La implementación satisface funcionalmente los contratos evaluados de:

* parsing UTC;
* ventanas por tiempo de evento;
* duplicados;
* aislamiento por comercio;
* eventos fuera de orden;
* lateness aceptada;
* eventos demasiado tardíos;
* pipeline Beam;
* estado por clave;
* expiración mediante timer;
* reintentos idempotentes;
* comportamiento append-only.

La suite oficial actualmente produce:

```text
12 passed, 1 failed
```

El único fallo ocurre en:

```text
test_trigger_policy_has_lateness_and_accumulating_panes
```

y no proviene de la configuración de la política temporal.

El proyecto utiliza Apache Beam `2.74.0`. En esta versión, `FixedWindows(60)` representa el tamaño mediante:

```text
apache_beam.utils.timestamp.Duration
```

La ejecución directa confirmó:

```text
float(size) = 60.0
size.micros = 60000000
hasattr(size, "seconds") = False

float(allowed_lateness) = 120.0
trigger = AfterWatermark
accumulation_mode = ACCUMULATING
```

El test proporcionado intenta acceder a:

```python
policy.windowing.windowfn.size.seconds
```

pero `Duration` no expone ese atributo en la versión fijada por el proyecto.

No se modificaron las pruebas oficiales ni se introdujo código artificial para fabricar dicho atributo. La política implementada mantiene la semántica requerida por la tarea.

## Validaciones realizadas

```text
parse_utc                                  PASS
assign_fixed_window                        PASS
duplicados                                 PASS
aislamiento por comercio                   PASS
eventos fuera de orden                     PASS
late aceptado                              PASS
evento demasiado tardío                    PASS
pipeline Beam                              PASS
estado por clave                           PASS
timer de limpieza                          PASS
reintentos idempotentes                    PASS
append-only                                PASS
TestStream adicional                       PASS
ruff                                       PASS
marimo check --strict                      PASS
```

## Evidencias

Las evidencias finales de ejecución se almacenarán en:

```text
evidencias/
```

e incluirán:

```text
01_pipeline_marimo.png
02_pruebas_finales.png
03_validaciones_finales.png
```

## Trade-offs

### Estado

Recordar `event_id` permite deduplicar, pero consume recursos del runner. Por eso el estado posee una política explícita de expiración.

### Lateness

Aceptar eventos tardíos mejora la completitud, pero prolonga la vida del estado y permite revisiones posteriores del resultado.

### Panes acumulativos

Cada pane representa una revisión completa del total de la ventana. Esto simplifica el contrato de un sink basado en `UPSERT`.

### Idempotencia

La garantía se limita al efecto lógico representado por:

```text
merchant_id|window_start
```

No se afirma una garantía global de `exactly once` sobre sistemas externos.

## Entrega

La entrega consiste en un único enlace público al repositorio de GitHub que contiene:

* `notebook.py` implementado;
* pruebas oficiales sin modificar;
* `tests/test_temporal.py`;
* instrucciones reproducibles;
* decisiones y trade-offs;
* evidencias finales de ejecución.
