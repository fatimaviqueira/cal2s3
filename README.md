# Cal2S3

Este proyecto contiene un script que extrae datos de una tabla MySQL, los formatea, los comprime y los guarda en carpetas en S3 según la fecha.

## Funcionamiento

1. Conecta a MySQL y ejecuta una consulta para obtener los datos
2. Procesa los resultados como stream para manejar grandes volúmenes de datos
3. Formatea los datos (añade campos adicionales, formatea fechas)
4. Comprime los datos con gzip
5. Guarda los datos comprimidos en S3 en carpetas organizadas por fecha

## Requisitos

- Node.js (se recomienda usar nvm para gestionar versiones)
- Acceso a una base de datos MySQL
- Acceso a un bucket de S3 en AWS
- Credenciales de AWS configuradas

## Instalación

1. Clona este repositorio
2. Instala las dependencias:

```bash
npm install
```

## Configuración

1. Crea un archivo `.env` en la raíz del proyecto (ya existe un ejemplo)
2. Configura las variables de entorno según tus necesidades:

```
# Configuración del entorno
ENVIRONMENT=development

# Configuración de AWS
AWS_REGION=eu-west-1
BUCKET_NAME=lam-development-teixo

# Configuración de MySQL
MYSQL_HOST=localhost
MYSQL_USER=root
MYSQL_PASSWORD=password
MYSQL_DATABASE=database_name

# Configuración de logs
LOG_LEVEL=info # Valores posibles: debug, info, warn, error
```

## Uso

Ejecuta el script con:

```bash
node copy.js [opciones]
```

Opciones disponibles:
- `--start=YYYY-MM-DD`: Fecha de inicio para filtrar registros
- `--end=YYYY-MM-DD`: Fecha de fin para filtrar registros
- `--limit=N`: Número máximo de registros a procesar
- `--batch-size=N`: Número de registros por archivo (0 para un solo archivo)
- `--test`: Modo test (genera datos aleatorios y los guarda localmente)
- `--test-db`: Modo test-db (obtiene datos de MySQL pero los guarda localmente)

Ejemplos:

```bash
# Procesar todos los registros
node copy.js

# Procesar registros de una fecha específica
node copy.js --start=2025-05-01 --end=2025-05-31

# Procesar un número limitado de registros
node copy.js --limit=1000

# Dividir los registros en lotes de 500 por archivo
node copy.js --batch-size=500

# Modo test: generar 1000 registros aleatorios y guardarlos localmente
node copy.js --test --limit=1000

# Modo test con lotes: generar 1000 registros en lotes de 200
node copy.js --test --limit=1000 --batch-size=200

# Modo test-db: obtener datos de MySQL y guardarlos localmente
node copy.js --test-db --start=2025-05-01 --end=2025-05-31

# Modo test-db con lotes: obtener datos de MySQL en lotes de 200 y guardarlos localmente
node copy.js --test-db --limit=1000 --batch-size=200
```

## Estructura de archivos en S3

Los archivos se guardan en S3 con la siguiente estructura:

```
cal/[año]/[mes]/[día]/[hora]/cal_[año][mes][día]_[hora][minuto]_[segundo]_[milisegundo]_[uuid].json.gz
```

Por ejemplo:

```
cal/2025/05/16/12/cal_20250516_1219_45_123_a1b2c3d4-e5f6-4g7h-8i9j-k0l1m2n3o4p5.json.gz
```

## Procesamiento de datos

El script utiliza streams para procesar los datos, lo que permite manejar grandes volúmenes de información sin consumir demasiada memoria. El proceso es el siguiente:

1. Se obtienen los datos de MySQL como un stream
2. Se procesan los registros uno a uno, añadiendo campos adicionales y formateando fechas
3. Se acumulan los registros y se convierten a JSON
4. Se comprime el JSON con gzip
5. Se guarda el archivo comprimido en S3

## Sistema de logs

El script incluye un sistema de logs con diferentes niveles:

- `debug`: Información detallada para depuración
- `info`: Información general sobre el proceso
- `warn`: Advertencias que no detienen el proceso
- `error`: Errores que pueden detener el proceso

Puedes configurar el nivel de logs en el archivo `.env` con la variable `LOG_LEVEL`.

## Manejo de errores

El script incluye manejo de errores para:
- Problemas de conexión a MySQL
- Errores en la consulta SQL
- Problemas al procesar los datos
- Errores al guardar en S3

En caso de error, se cierra la conexión a MySQL (si está abierta) y se devuelve un mensaje de error.
