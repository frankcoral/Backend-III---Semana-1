# Banco XYZ - Migración de Procesos Legacy con Spring Batch

## Descripción del proyecto

Este proyecto corresponde a la actividad de la Semana 1 de **Desarrollo Backend III (PBY2203)**.

El objetivo es modernizar procesos batch de un sistema legacy del **Banco XYZ**, utilizando **Spring Batch** para leer información desde archivos CSV, aplicar transformaciones y validaciones sobre los datos y persistir los resultados procesados en una base de datos relacional MySQL.

La solución implementa tres procesos principales:

1. Reporte de transacciones diarias.
2. Cálculo de intereses mensuales.
3. Generación de estados de cuenta anuales.

Los datos utilizados corresponden a los archivos proporcionados en el repositorio `bank_legacy_data`.

---

## Tecnologías utilizadas

* Java 17
* Spring Boot 4.1.0
* Spring Batch
* Spring JDBC
* Maven
* MySQL Server 8
* MySQL Workbench
* Visual Studio Code

---

## Arquitectura de procesamiento

Los procesos utilizan la arquitectura de Spring Batch basada principalmente en:

```text
Archivo CSV
    ↓
ItemReader
    ↓
ItemProcessor
    ↓
ItemWriter
    ↓
MySQL
```

Cada Job está dividido en Steps independientes que permiten organizar las distintas etapas del procesamiento.

Los `ItemReader` realizan la lectura de los archivos CSV, los `ItemProcessor` contienen las reglas de validación y transformación, y los `ItemWriter` persisten los resultados procesados en MySQL.

---

## Estructura del proyecto

```text
bank-batch-migration/
│
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── com/bancoxyz/batch/
│   │   │       │
│   │   │       ├── config/
│   │   │       │   ├── TransaccionesJobConfig.java
│   │   │       │   ├── InteresesJobConfig.java
│   │   │       │   └── EstadosCuentaJobConfig.java
│   │   │       │
│   │   │       ├── model/
│   │   │       │   ├── Transaccion.java
│   │   │       │   ├── CuentaInteres.java
│   │   │       │   └── MovimientoAnual.java
│   │   │       │
│   │   │       ├── processor/
│   │   │       │   ├── TransaccionProcessor.java
│   │   │       │   ├── CuentaInteresProcessor.java
│   │   │       │   └── MovimientoAnualProcessor.java
│   │   │       │
│   │   │       └── BankBatchMigrationApplication.java
│   │   │
│   │   └── resources/
│   │       ├── data/
│   │       │   ├── transacciones.csv
│   │       │   ├── intereses.csv
│   │       │   └── cuentas_anuales.csv
│   │       │
│   │       ├── application.properties
│   │       └── schema.sql
│   │
│   └── test/
│
├── output/
│   └── estados_cuenta_anuales.csv
│
├── pom.xml
├── mvnw
├── mvnw.cmd
└── README.md
```

---

# Procesos Batch implementados

## 1. Reporte de Transacciones Diarias

### Job

```text
transaccionesJob
```

Este Job procesa el archivo:

```text
src/main/resources/data/transacciones.csv
```

Su objetivo es analizar las transacciones bancarias diarias, detectar anomalías y generar un resumen general del procesamiento.

### Steps

```text
transaccionesJob
│
├── limpiarTransaccionesStep
│
├── procesarTransaccionesStep
│       ├── ItemReader
│       ├── ItemProcessor
│       └── ItemWriter
│
└── resumenTransaccionesStep
```

### Validaciones realizadas

El `TransaccionProcessor` verifica:

* existencia de fecha;
* existencia de monto;
* montos negativos;
* montos iguales a cero;
* existencia del tipo de transacción;
* tipos permitidos `debito` y `credito`;
* detección de transacciones duplicadas.

Las transacciones que presentan problemas no son eliminadas. Se almacenan indicando que son anómalas y registrando el motivo correspondiente.

Ejemplo:

```text
Monto: -200
Anómala: true
Motivo: Monto negativo o cero
```

### Tablas utilizadas

```text
transacciones_procesadas
resumen_transacciones
```

La tabla `transacciones_procesadas` contiene cada transacción junto con el resultado de sus validaciones.

La tabla `resumen_transacciones` almacena:

* total de transacciones procesadas;
* total de transacciones válidas;
* total de anomalías;
* total de créditos;
* total de débitos;
* fecha de ejecución.

### Resultado obtenido

Durante las pruebas realizadas se obtuvo:

```text
Total procesadas : 10
Total válidas    : 7
Total anómalas   : 3
Total créditos   : 4500.00
Total débitos    : 4700.00
```

---

# 2. Cálculo de Intereses Mensuales

## Job

```text
interesesJob
```

Este Job procesa el archivo:

```text
src/main/resources/data/intereses.csv
```

Su objetivo es calcular intereses sobre cuentas de ahorro y préstamos y almacenar el nuevo saldo resultante.

### Steps

```text
interesesJob
│
├── limpiarInteresesStep
│
├── procesarInteresesStep
│       ├── ItemReader
│       ├── ItemProcessor
│       └── ItemWriter
│
└── resumenInteresesStep
```

### Propuesta técnica para tasas de interés

Debido a que los datos de entrada y las instrucciones de la actividad no indican tasas específicas, para esta implementación se definieron las siguientes tasas mensuales como parte de la propuesta técnica:

```text
Cuenta de ahorro   → 1%
Cuenta de préstamo → 2%
```

Estas tasas se utilizan exclusivamente con fines de demostración del proceso batch.

### Fórmula aplicada

```text
Interés = Saldo inicial × Tasa de interés

Saldo final = Saldo inicial + Interés
```

Ejemplo:

```text
Saldo inicial: 5000.00
Tipo: ahorro
Tasa: 1%

Interés calculado: 50.00
Saldo final: 5050.00
```

### Validaciones realizadas

El `CuentaInteresProcessor` verifica:

* nombre obligatorio;
* saldo obligatorio;
* saldo mayor que cero;
* edad válida;
* tipo de cuenta válido;
* únicamente cuentas de `ahorro` y `prestamo`.

Cuando una cuenta presenta errores, se registra como inválida junto con el motivo correspondiente y no se aplica un cálculo de interés válido.

Por ejemplo:

```text
Tipo: hipoteca
Resultado: inválido
Motivo: Tipo de cuenta no válido para cálculo de intereses
```

### Tablas utilizadas

```text
intereses_procesados
resumen_intereses
```

La tabla `intereses_procesados` almacena:

* identificador de cuenta;
* nombre;
* saldo inicial;
* edad;
* tipo;
* tasa aplicada;
* interés calculado;
* saldo final;
* estado de validación;
* motivo de error.

### Resultado obtenido

Durante la ejecución se obtuvo:

```text
Total procesadas : 8
Total válidas    : 6
Total inválidas  : 2
Interés total    : 900.00
```

---

# 3. Generación de Estados de Cuenta Anuales

## Job

```text
estadosCuentaAnualesJob
```

Este Job procesa:

```text
src/main/resources/data/cuentas_anuales.csv
```

Su objetivo es procesar los movimientos bancarios anuales, verificar su consistencia, consolidar los movimientos de cada cuenta y generar información que pueda utilizarse para auditoría.

### Steps

```text
estadosCuentaAnualesJob
│
├── limpiarEstadosCuentaStep
│
├── procesarMovimientosAnualesStep
│       ├── ItemReader
│       ├── ItemProcessor
│       └── ItemWriter
│
├── generarEstadosCuentaStep
│
└── exportarInformeAuditoriaStep
```

### Validaciones realizadas

El `MovimientoAnualProcessor` verifica:

* fecha obligatoria;
* descripción obligatoria;
* monto obligatorio;
* monto distinto de cero;
* tipo de movimiento válido;
* coherencia entre el tipo de transacción y el signo del monto.

Los movimientos aceptados son:

```text
deposito
retiro
compra
```

También se valida la relación entre operación y monto:

```text
deposito → monto positivo
retiro   → monto negativo
compra   → monto negativo
```

Por lo tanto, un retiro de `-500` no se considera un error, ya que el signo negativo es coherente con la operación.

En cambio:

```text
deposito → 0.00
```

es considerado inválido.

### Tablas utilizadas

```text
movimientos_anuales
estados_cuenta_anuales
```

`movimientos_anuales` contiene todos los movimientos procesados junto con el resultado de las validaciones.

`estados_cuenta_anuales` consolida la información de cada cuenta e incluye:

* cantidad total de movimientos;
* movimientos válidos;
* movimientos inválidos;
* total de depósitos;
* total de cargos;
* saldo neto;
* fecha de generación.

### Informe para auditoría

Además de persistir los resultados en MySQL, el Job genera automáticamente:

```text
output/estados_cuenta_anuales.csv
```

Este archivo contiene el resumen anual de cada cuenta y puede ser utilizado como salida para procesos de auditoría.

### Resultado obtenido

Durante las pruebas se obtuvo:

```text
Total movimientos  : 9
Movimientos válidos: 8
Mov. inválidos     : 1
Cuentas procesadas : 8
```

El archivo de auditoría fue generado correctamente en:

```text
output/estados_cuenta_anuales.csv
```

---

# Manejo de errores y consistencia de datos

Los archivos legacy contienen información que puede presentar inconsistencias.

La solución utiliza los `ItemProcessor` para validar y normalizar los datos antes de su persistencia.

Las principales reglas implementadas incluyen:

* detección de valores nulos o vacíos;
* validación de montos;
* normalización de tipos de operación;
* detección de registros duplicados;
* validación de tipos de cuenta;
* control de saldos iguales a cero;
* validación de edades;
* validación de coherencia entre tipo de movimiento y signo del monto.

Los registros con problemas se mantienen en la base de datos con indicadores de validez y descripción del error, permitiendo conservar la trazabilidad de los datos provenientes del sistema legacy.

---

# Persistencia

Se utiliza **MySQL** como base de datos relacional.

Nombre de la base:

```text
bank_batch_db
```

Las tablas de negocio son creadas automáticamente mediante:

```text
src/main/resources/schema.sql
```

Spring Batch utiliza además sus propias tablas internas para almacenar información relacionada con la ejecución de Jobs y Steps.

---

# Configuración de la base de datos

Antes de ejecutar el proyecto se debe crear la base:

```sql
CREATE DATABASE bank_batch_db
CHARACTER SET utf8mb4
COLLATE utf8mb4_unicode_ci;
```

La conexión se configura en:

```text
src/main/resources/application.properties
```

Configuración utilizada:

```properties
spring.application.name=bank-batch-migration

spring.datasource.url=jdbc:mysql://localhost:3306/bank_batch_db
spring.datasource.username=root
spring.datasource.password=${DB_PASSWORD}
spring.datasource.driver-class-name=com.mysql.cj.jdbc.Driver

spring.batch.jdbc.initialize-schema=always
spring.batch.job.enabled=true

spring.sql.init.mode=always
```

Por seguridad, la contraseña de MySQL no se almacena directamente en el proyecto.

Se utiliza la variable de entorno:

```text
DB_PASSWORD
```

---

# Requisitos para ejecutar el proyecto

Se necesita tener instalado:

* Java 17 o superior compatible con el proyecto.
* Maven o utilizar Maven Wrapper incluido.
* MySQL Server.
* Una instancia MySQL funcionando en el puerto `3306`.
* La base de datos `bank_batch_db`.

---

# Ejecución del proyecto

## 1. Configurar la contraseña de MySQL

En Windows PowerShell:

```powershell
$env:DB_PASSWORD="CONTRASEÑA_MYSQL"
```

Reemplazar `CONTRASEÑA_MYSQL` por la contraseña correspondiente al usuario configurado en MySQL.

---

## 2. Seleccionar el Job

En:

```text
src/main/resources/application.properties
```

utilizar:

### Transacciones diarias

```properties
spring.batch.job.name=transaccionesJob
```

### Intereses mensuales

```properties
spring.batch.job.name=interesesJob
```

### Estados de cuenta anuales

```properties
spring.batch.job.name=estadosCuentaAnualesJob
```

Debe ejecutarse un Job a la vez.

---

## 3. Compilar el proyecto

Desde la carpeta raíz:

```powershell
.\mvnw.cmd clean compile
```

El resultado esperado es:

```text
BUILD SUCCESS
```

---

## 4. Ejecutar el Job

```powershell
.\mvnw.cmd spring-boot:run
```

Durante la ejecución, Spring Batch mostrará los Steps ejecutados y el resumen correspondiente al Job seleccionado.

Una ejecución correcta finaliza con un estado:

```text
COMPLETED
```

y Maven muestra:

```text
BUILD SUCCESS
```

---

# Archivos de entrada

Los archivos legacy utilizados se encuentran en:

```text
src/main/resources/data/
```

Archivos:

```text
transacciones.csv
intereses.csv
cuentas_anuales.csv
```

Estos archivos corresponden a los datos proporcionados para la actividad y se mantienen sin modificar para permitir que las validaciones de Spring Batch detecten sus inconsistencias.

---

# Archivos de salida

El proceso de estados de cuenta anuales genera:

```text
output/estados_cuenta_anuales.csv
```

Este archivo representa el informe consolidado utilizado como salida para auditoría.

Los otros resultados se almacenan directamente en MySQL.

---

# Evidencias de ejecución

Las evidencias del proyecto muestran:

## Job de transacciones

* ejecución de `transaccionesJob`;
* ejecución de sus Steps;
* detección de anomalías;
* resumen generado;
* datos persistidos en MySQL.

## Job de intereses

* ejecución de `interesesJob`;
* cálculo de intereses;
* cálculo del saldo final;
* registros inválidos;
* resumen de intereses;
* persistencia en MySQL.

## Job de estados de cuenta anuales

* ejecución de `estadosCuentaAnualesJob`;
* procesamiento de movimientos;
* validaciones;
* consolidación por cuenta;
* persistencia en MySQL;
* generación del archivo CSV para auditoría.

---

# Resultados generales

Los tres procesos Spring Batch se ejecutaron correctamente.

```text
Transacciones
10 procesadas
7 válidas
3 anómalas

Intereses
8 procesadas
6 válidas
2 inválidas
900.00 de interés total

Estados anuales
9 movimientos
8 válidos
1 inválido
8 cuentas procesadas
```

Los Jobs finalizaron con estado:

```text
COMPLETED
```

y los datos generados fueron persistidos correctamente en MySQL.

---

# Propuesta técnica

La solución busca reemplazar procesos batch legacy mediante una arquitectura modular basada en Spring Batch.

La separación entre Reader, Processor y Writer permite:

* aislar responsabilidades;
* facilitar el mantenimiento;
* aplicar reglas de validación antes de persistir información;
* conservar la trazabilidad de registros inconsistentes;
* dividir cada proceso en Steps;
* generar salidas reutilizables;
* facilitar futuras modificaciones o ampliaciones del sistema.

El almacenamiento de registros inválidos junto con el motivo del error permite analizar la calidad de los datos legacy sin perder la información original procesada.

La utilización de MySQL permite persistir tanto los resultados transformados como los resúmenes generados por cada proceso.

---

# Repositorio

Repositorio GitHub:

```text
https://github.com/frankcoral/Backend-III---Semana-1
```

---

## Información académica

**Asignatura:** Desarrollo Backend III
**Código:** PBY2203
**Experiencia:** Exp 1
**Semana:** 1
**Actividad:** Analizando la arquitectura batch para procesar datos
**Grupo:** Grupo 14
