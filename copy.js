/**
 * copy.js
 *
 * Este script extrae datos de una tabla MySQL, los formatea,
 * los comprime y los guarda en carpetas en S3 según la fecha.
 *
 * Funcionamiento:
 * 1. Conecta a MySQL y ejecuta una consulta para obtener los datos
 * 2. Procesa los resultados agrupándolos por intervalos de 10 minutos o por lotes de tamaño fijo
 * 3. Formatea los datos (añade campos adicionales, formatea fechas)
 * 4. Comprime los datos con gzip
 * 5. Guarda los datos comprimidos en S3 en carpetas organizadas por fecha
 */

// Importamos las librerías necesarias
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import mysql from 'mysql2';
import zlib from 'zlib';
import dotenv from 'dotenv';
import { fileURLToPath } from 'url';

// Cargamos las variables de entorno
dotenv.config();

console.log('Cargando variables de entorno...');
console.log(`Entorno: ${process.env.ENVIRONMENT}`);
console.log(`Bucket: ${process.env.BUCKET_NAME}`);
console.log(`Región: ${process.env.AWS_REGION}`);
console.log(`MySQL Host: ${process.env.MYSQL_HOST}`);
console.log(`MySQL Port: ${process.env.MYSQL_PORT}`);
console.log(`MySQL User: ${process.env.MYSQL_USER}`);
console.log(`MySQL Database: ${process.env.MYSQL_DATABASE}`);
console.log(`Log Level: ${process.env.LOG_LEVEL}`);

// Configuración del entorno
const ENVIRONMENT = process.env.ENVIRONMENT || 'development';
const BUCKET_NAME = process.env.BUCKET_NAME || `lam-${ENVIRONMENT}-teixo`;
const AWS_REGION = process.env.AWS_REGION || 'eu-west-1';
const LOG_LEVEL = process.env.LOG_LEVEL || 'info';

// Configuración de MySQL
const MYSQL_CONFIG = {
    host: process.env.MYSQL_HOST || 'localhost',
    port: process.env.MYSQL_PORT || '3306',
    user: process.env.MYSQL_USER || 'root',
    password: process.env.MYSQL_PASSWORD || '',
    database: process.env.MYSQL_DATABASE || 'database_name',
    // Configuración para manejar grandes conjuntos de datos
    supportBigNumbers: true,
    bigNumberStrings: true
};

// Cliente de S3 para subir archivos
const s3Client = new S3Client({ region: AWS_REGION });

/**
 * Sistema de logs con niveles
 */
const LogLevels = {
    DEBUG: 0,
    INFO: 1,
    WARN: 2,
    ERROR: 3
};

const Logger = {
    currentLevel: LogLevels[LOG_LEVEL.toUpperCase()] || LogLevels.INFO,

    _log: function(level, ...args) { // Acepta múltiples argumentos
        // Helper para obtener el timestamp en formato YYYY-MM-DD HH:MM:SS.mmm
        const getFormattedTimestamp = () => {
            const d = new Date();
            const pad = (num, size = 2) => num.toString().padStart(size, '0');
            return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}.${pad(d.getMilliseconds(), 3)}`;
        };

        // Construir el mensaje a partir de los argumentos
        // Si un argumento es un objeto Error, se usa su stack.
        const messageParts = args.map(arg => {
            if (arg instanceof Error) {
                return arg.stack; // Usar el stacktrace completo
            }
            // Para otros objetos, podríamos usar JSON.stringify o similar si es necesario.
            if (typeof arg === 'object' && arg !== null) {
                try {
                    // Intentar convertir a JSON, útil para objetos simples
                    return JSON.stringify(arg);
                } catch (e) {
                    // Si falla JSON.stringify (ej. objetos circulares), usar toString()
                    return arg.toString();
                }
            }
            return arg; // Para strings, números, etc.
        });
        const message = messageParts.join(' ');

        // Seleccionar la función de consola adecuada según el nivel
        const logFunction = level === 'ERROR' ? console.error :
                            level === 'WARN' ? console.warn :
                            console.log;

        logFunction(`${getFormattedTimestamp()} [${level}] ${message}`);
    },

    debug: function(...args) { // Aceptar múltiples argumentos
        if (this.currentLevel <= LogLevels.DEBUG) {
            this._log('DEBUG', ...args);
        }
    },

    info: function(...args) { // Aceptar múltiples argumentos
        if (this.currentLevel <= LogLevels.INFO) {
            this._log('INFO', ...args);
        }
    },

    warn: function(...args) { // Aceptar múltiples argumentos
        if (this.currentLevel <= LogLevels.WARN) {
            this._log('WARN', ...args);
        }
    },

    error: function(...args) { // Aceptar múltiples argumentos
        if (this.currentLevel <= LogLevels.ERROR) {
            this._log('ERROR', ...args);
        }
    }
};

/**
 * Genera un UUID v4 para identificar de forma única cada archivo
 * @returns {string} UUID generado
 */
const uuidv4 = () => {
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
        var r = Math.random() * 16 | 0,
            v = c === 'x' ? r : (r & 0x3 | 0x8);
        return v.toString(16);
    });
};

/**
 * Descompone una fecha en sus componentes (año, mes, día, etc.)
 * @param {Date} date - Fecha a descomponer
 * @returns {Object} Objeto con los componentes de la fecha
 */
const decomposeDate = (date) => {
    // Función para rellenar un número con ceros a la izquierda
    const padNumber = (number, digits) => number.toString().padStart(digits, '0');

    // Descomponer la fecha y hora en sus componentes
    const year = date.getFullYear();
    const month = padNumber(date.getMonth() + 1, 2);
    const day = padNumber(date.getDate(), 2);
    const hours = padNumber(date.getHours(), 2);
    const minutes = padNumber(date.getMinutes(), 2);
    const seconds = padNumber(date.getSeconds(), 2);
    const milliseconds = padNumber(date.getMilliseconds(), 3);

    return { year, month, day, hours, minutes, seconds, milliseconds };
};

/**
 * Genera un nombre de archivo basado en una fecha y un tipo, agrupado por hora.
 * @param {string} type - Tipo de datos (usado como prefijo en el nombre del archivo)
 * @param {Date} date - Fecha a utilizar para el nombre del archivo. Los minutos, segundos y ms se usarán para el UUID si es necesario, pero la carpeta es horaria.
 * @param {boolean} isHourlyFile - Indica si el archivo representa toda la hora (minutos y segundos serán 00).
 * @returns {string} Ruta completa del archivo incluyendo carpetas organizadas por fecha y hora.
 */
const getTimestampFilename = (type, date, isHourlyFile = false) => {
    // Descomponer la fecha y hora en sus componentes
    const { year, month, day, hours } = decomposeDate(date);
    let minutes = date.getMinutes();
    let seconds = date.getSeconds();
    let milliseconds = date.getMilliseconds();

    if (isHourlyFile) {
        minutes = 0;
        seconds = 0;
        // Los milisegundos pueden dejarse o ponerse a 0, el UUID ya da unicidad.
        // Para un nombre "limpio" de hora, podríamos omitirlos o ponerlos a 0.
        // Por ahora, los mantenemos del primer registro si es un lote parcial, o 000 si es el archivo de la hora.
        milliseconds = 0; // Para un nombre de archivo representativo de la hora.
    }

    const pad = (num, size = 2) => num.toString().padStart(size, '0');

    // Crear el nombre del archivo con la fecha y hora
    // La estructura de carpetas es YYYY/MM/DD/HH
    // El nombre del archivo es type_YYYYMMDD_HHMM_SS_mmm_uuid.json.gz
    // Si isHourlyFile es true, MM y SS serán 00.
    return `${year}/${month}/${day}/${pad(hours)}/${type}_${year}${month}${day}_${pad(hours)}${pad(minutes)}_${pad(seconds)}_${pad(milliseconds, 3)}_${uuidv4()}.json.gz`;
};

/**
 * Formatea una fecha para que sea compatible con el formato esperado
 * @param {string} dateString - Fecha en formato string
 * @returns {string} Fecha formateada
 */
const formatDate = (dateString) => {
    if (!dateString) return null;

    // Crear un objeto Date a partir de la cadena de fecha original
    const date = new Date(dateString);
    // Descomponer la fecha y hora en sus componentes
    const { year, month, day, hours, minutes, seconds, milliseconds } = decomposeDate(date);
    // Formatear la fecha en el formato necesario
    return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}.${milliseconds}`;
};

/**
 * Conecta a la base de datos MySQL
 * @returns {Object} Conexión a MySQL
 */
const connectToMySQL = () => {
    try {
        Logger.info(`Conectando a MySQL en ${MYSQL_CONFIG.host}...`);
        const connection = mysql.createConnection(MYSQL_CONFIG);
        Logger.info('Conexión a MySQL establecida correctamente');
        return connection;
    } catch (error) {
        Logger.error('Error al conectar a MySQL:', error); // Modificado para pasar el objeto error
        throw error;
    }
};

/**
 * Procesa un registro
 * @param {Object} record - Registro a procesar
 * @param {string} type - Tipo de datos
 * @returns {Object} Registro procesado
 */
const processRecord = (record, type) => {
    // Añadir campos adicionales (excepto s3file, que se añadirá más tarde)
    record.uuid = record.uuid || uuidv4();

    // Formatear fechas
    if (record.created_at) {
        record.created_at = formatDate(record.created_at);
    }
    if (record.updated_at) {
        record.updated_at = formatDate(record.updated_at);
    }

    // Convertir ajax_call a booleano si es un valor numérico
    if (record.ajax_call !== undefined) {
        record.ajax_call = Boolean(record.ajax_call);
    }

    return record;
};

/**
 * Procesa un lote de registros y los guarda en un archivo
 * @param {Array} records - Registros a procesar
 * @param {string} type - Tipo de datos (usado como prefijo en el nombre del archivo)
 * @param {boolean} saveLocally - Indica si se debe guardar localmente o en S3
 * @param {string} hourKey - Clave de la hora para agrupar registros (opcional, formato YYYY-MM-DD-HH)
 * @returns {Promise<Object>} Información sobre el lote procesado
 */
const processBatch = async (records, type, saveLocally, hourKey = null) => {
    if (records.length === 0) return null;

    // Obtener la fecha del primer registro para el nombre del archivo
    const firstRecord = records[0];
    // Usamos la fecha del primer registro si no hay hourKey, o la hourKey para la base del nombre del archivo.
    const baseDate = hourKey
        ? new Date(parseInt(hourKey.split('-')[0]), parseInt(hourKey.split('-')[1]) - 1, parseInt(hourKey.split('-')[2]), parseInt(hourKey.split('-')[3]))
        : new Date(firstRecord.created_at);

    // Generar el nombre del archivo.
    // Si se proporciona hourKey, significa que este archivo es para toda esa hora.
    // Si no, es un lote (batchSize > 0) y el nombre del archivo se basa en el primer registro.
    const isHourlyFile = !!hourKey;
    const fileName = `${type}/${getTimestampFilename(type, baseDate, isHourlyFile)}`;

    // Añadir el campo s3file a todos los registros con la ruta completa
    const fullPath = `${BUCKET_NAME}/${fileName}`;
    for (const record of records) {
        record.s3file = fullPath;
    }

    // Convertir los registros acumulados a JSON
    const jsonContent = JSON.stringify(records, null, 2);

    // Comprimir el contenido
    const compressedContent = zlib.gzipSync(jsonContent, { level: 9 });
    Logger.debug(`Contenido comprimido, tamaño: ${compressedContent.length} bytes`);

    // Guardar el contenido comprimido
    if (saveLocally) {
        await saveToLocalFile(compressedContent, fileName);
    } else {
        await saveToS3(compressedContent, fileName);
    }

    return {
        count: records.length,
        date: new Date(firstRecord.created_at), // Corregido: usar la fecha del primer registro
        fileName
    };
};

/**
 * Agrupa registros por hora
 * @param {Object} record - Registro a agrupar
 * @returns {string} Clave de la hora para agrupar (formato YYYY-MM-DD-HH)
 */
const getHourKey = (record) => {
    const createdAt = new Date(record.created_at);
    const pad = (num) => num.toString().padStart(2, '0');
    // Clave basada en año, mes, día y hora
    return `${createdAt.getFullYear()}-${pad(createdAt.getMonth() + 1)}-${pad(createdAt.getDate())}-${pad(createdAt.getHours())}`;
};

/**
 * Guarda datos en S3
 * @param {Buffer} content - Contenido a guardar
 * @param {string} fileName - Nombre del archivo
 * @returns {Promise<void>}
 */
const saveToS3 = async (content, fileName) => {
    const params = {
        Bucket: BUCKET_NAME,
        Key: fileName,
        Body: content,
        ContentType: 'application/gzip'
    };

    try {
        Logger.info(`Iniciando envío a S3 (bucket: ${BUCKET_NAME}, archivo: ${fileName})...`);
        const command = new PutObjectCommand(params);
        await s3Client.send(command);
        Logger.info(`Archivo guardado en ${BUCKET_NAME}/${fileName}`);
    } catch (error) {
        Logger.error('Error al guardar el archivo en S3:', error); // Modificado
        throw error;
    }
};

/**
 * Construye la consulta SQL con filtros de fecha
 * @param {string} queryNumber - Número de consulta (1, 2, o 3)
 * @param {string} startDate - Fecha de inicio (YYYY-MM-DD)
 * @param {string} endDate - Fecha de fin (YYYY-MM-DD)
 * @param {number} limit - Límite de registros
 * @param {string} [id] - ID específico para filtrar (opcional)
 * @returns {Object} Objeto con la consulta y los parámetros
 */
const buildQuery = (queryNumber, startDate, endDate, limit, id) => {
    // Consulta base para cada tabla
    let baseSelect = `
        SELECT
            cal.id,
            cal.controller,
            cal.action,
            cal.browser,
            cal.http_method,
            cal.ajax_call,
            cal.params,
            cal.time,
            cal.user,
            cal.ip,
            cal.account_id,
            cal.created_at,
            cal.updated_at,
            cal.app_version,
            cal.infered_object_id,
            cal.user_id,
            acc.client_code as account_client_code,
            acc.client_name as account_client_name,
            acc.client_type as account_client_type,
            acc.description as account_description
    `;

    if (queryNumber === '3') {
        baseSelect += `,
            cal.request_trace_id,
            cal.error_message,
            cal.server_instance_name
        `;
    } else if (queryNumber === '2') {
        baseSelect += `,
            cal.server_instance_name
        `;
    }

    // Construir la consulta para controller_actions_logs_old
    let query1 = `
        ${baseSelect}
        FROM controller_actions_logs_old cal
        LEFT JOIN accounts acc ON cal.account_id = acc.id
        WHERE 1=1
    `;

    // Construir la consulta para controller_actions_logs_old2
    let query2 = `
        ${baseSelect}
        FROM controller_actions_logs_old2 cal
        LEFT JOIN accounts acc ON cal.account_id = acc.id
        WHERE 1=1
    `;

    // Construir la consulta para controller_actions_logs (solo hasta el ID 1040103545)
    let query3 = `
        ${baseSelect}
        FROM controller_actions_logs cal
        LEFT JOIN accounts acc ON cal.account_id = acc.id
        WHERE 1=1
        AND cal.id <= 1040103545
    `;

    const params = [];

    // Dependiendo de queryNumber, seleccionamos la consulta adecuada
    let query;
    if (queryNumber === '1') {
        query = query1;
    } else if (queryNumber === '2') {
        query = query2;
    } else if (queryNumber === '3') {
        query = query3;
    } else {
        throw new Error(`Número de consulta inválido: ${queryNumber}. Debe ser 1, 2 o 3.`);
    }

    // Añadir filtro de fecha de inicio si se especifica
    if (startDate) {
        query += ` AND cal.created_at >= ?`;
        params.push(startDate);
    }

    // Añadir filtro de fecha de fin si se especifica
    if (endDate) {
        query += ` AND cal.created_at <= ?`;
        params.push(endDate);
    }

    // Añadir filtro de ID si se especifica
    if (id) {
        query += ` AND cal.id = ?`;
        params.push(id);
    }

    // Añadir límite si se especifica
    if (limit) {
        query += ` LIMIT ?`;
        params.push(limit);
    }

    return { query, params };
};

/**
 * Genera un objeto aleatorio para pruebas
 * @returns {Object} Objeto aleatorio
 */
const generateRandomObject = () => {
    const now = new Date();
    const id = Math.floor(Math.random() * 10000);

    return {
        id,
        controller: ['UsersController', 'ProductsController', 'OrdersController'][Math.floor(Math.random() * 3)],
        action: ['index', 'show', 'create', 'update', 'delete'][Math.floor(Math.random() * 5)],
        browser: ['Chrome', 'Firefox', 'Safari', 'Edge'][Math.floor(Math.random() * 4)],
        http_method: ['GET', 'POST', 'PUT', 'DELETE'][Math.floor(Math.random() * 4)],
        ajax_call: Math.random() > 0.5,
        params: JSON.stringify({ id: Math.floor(Math.random() * 100) }),
        time: Math.random() * 2,
        user: `user${Math.floor(Math.random() * 100)}`,
        ip: `192.168.1.${Math.floor(Math.random() * 255)}`,
        account_id: Math.floor(Math.random() * 50),
        created_at: new Date(now - Math.floor(Math.random() * 30 * 24 * 60 * 60 * 1000)),
        updated_at: now,
        app_version: `1.${Math.floor(Math.random() * 10)}.${Math.floor(Math.random() * 10)}`,
        infered_object_id: Math.floor(Math.random() * 1000),
        user_id: Math.floor(Math.random() * 100),
        server_instance_name: `server-${Math.floor(Math.random() * 10)}`,
        request_trace_id: uuidv4(),
        error_message: Math.random() > 0.8 ? `Error ${Math.floor(Math.random() * 100)}` : null,
        account_client_code: `CLI-${Math.floor(Math.random() * 1000)}`,
        account_client_name: `Cliente ${Math.floor(Math.random() * 100)}`,
        account_client_type: ['Empresa', 'Particular', 'Administración'][Math.floor(Math.random() * 3)],
        account_description: `Descripción del cliente ${Math.floor(Math.random() * 100)}`
    };
};

/**
 * Genera un array de objetos aleatorios para pruebas
 * @param {number} count - Número de objetos a generar
 * @returns {Array} Array de objetos aleatorios
 */
const generateRandomObjects = (count) => {
    const objects = [];
    for (let i = 0; i < count; i++) {
        objects.push(generateRandomObject());
    }
    return objects;
};

/**
 * Guarda datos en un archivo local (modo test)
 * @param {Buffer} content - Contenido a guardar
 * @param {string} fileName - Nombre del archivo
 * @returns {Promise<void>}
 */
const saveToLocalFile = async (content, fileName) => {
    const fs = await import('fs/promises');
    const path = await import('path');

    try {
        // Crear directorio si no existe
        const dirPath = path.dirname(fileName);
        await fs.mkdir(dirPath, { recursive: true });

        // Guardar archivo
        await fs.writeFile(fileName, content);
        Logger.info(`Archivo guardado en ${fileName}`);
    } catch (error) {
        Logger.error('Error al guardar el archivo local:', error); // Modificado
        throw error;
    }
};

/**
 * Función principal que ejecuta todo el proceso
 * @param {Object} options - Opciones de ejecución
 * @param {string} options.queryNumber - Número de consulta (1, 2, o 3)
 * @param {string} options.startDate - Fecha de inicio (YYYY-MM-DD)
 * @param {string} options.endDate - Fecha de fin (YYYY-MM-DD)
 * @param {number} options.limit - Límite de registros a procesar
 * @param {string} [options.id] - ID específico para filtrar (opcional)
 * @param {boolean} options.test - Modo test (genera datos aleatorios)
 * @param {boolean} options.testDb - Modo test-db (usa MySQL pero guarda localmente)
 * @param {number} options.batchSize - Tamaño del lote de registros por archivo (0 para un solo archivo)
 * @returns {Promise<Object>} Resultado de la operación
 */
const main = async (options = {}) => {
    // Desestructurar 'id' de las opciones
    const { queryNumber, startDate, endDate, limit, id, test = false, testDb = false, batchSize = 0} = options;

    // Determinar si se guardarán los archivos localmente
    const saveLocally = test || testDb;

    Logger.info(`Iniciando proceso con opciones: ${JSON.stringify(options)}`);
    let connection;
    let filesCreated = 0;
    let totalRecords = 0;
    let overallLastProcessedDate = null; // Para almacenar la fecha del último registro procesado globalmente

    // Helper para formatear la fecha para logs, si no es null
    const formatOverallDateForLog = () => overallLastProcessedDate ? ` (último registro procesado: ${overallLastProcessedDate})` : '';

    try {
        // Generar nombre de archivo base (siempre de tipo 'cal')
        const type = 'cal';

        // Si no se especificó un tamaño de lote, usamos un valor predeterminado para evitar problemas de memoria
        const effectiveBatchSize = batchSize || 0; // 0 significa agrupar por hora
        Logger.info(`Tamaño de lote efectivo: ${effectiveBatchSize || 'agrupación por hora'}`);

        if (test) {
            // Modo test: generar datos aleatorios
            const testLimit = limit || 100;
            Logger.info(`Modo TEST: Generando ${testLimit} registros aleatorios`);

            // Generar datos aleatorios
            const randomObjects = generateRandomObjects(testLimit);
            Logger.info(`Generados ${randomObjects.length} registros aleatorios`);

            // Procesar los registros
            const processedRecords = randomObjects.map(record => {
                const processed = processRecord(record, type);
                // Actualizar la fecha del último registro procesado si este registro tiene una fecha válida
                if (processed.created_at) {
                    overallLastProcessedDate = processed.created_at;
                }
                return processed;
            });
            Logger.info(`Registros de prueba procesados${formatOverallDateForLog()}`);

            // Si se usa agrupación por hora
            if (effectiveBatchSize === 0) {
                // Agrupar por hora
                const recordsByHour = new Map();

                // Agrupar registros por hora
                for (const record of processedRecords) {
                    const hourKey = getHourKey(record); // Usar getHourKey
                    if (!recordsByHour.has(hourKey)) {
                        recordsByHour.set(hourKey, []);
                    }
                    recordsByHour.get(hourKey).push(record);
                }

                // Procesar cada grupo de horas
                for (const [hourKey, hourRecords] of recordsByHour.entries()) {
                    // Actualizar overallLastProcessedDate con el último registro de este grupo horario si es necesario
                    if (hourRecords.length > 0 && hourRecords[hourRecords.length - 1].created_at) {
                        overallLastProcessedDate = hourRecords[hourRecords.length - 1].created_at;
                    }
                    Logger.info(`Procesando lote de ${hourRecords.length} registros de la hora ${hourKey}${formatOverallDateForLog()}`);

                    // Procesar el lote pasando la clave de la hora para mantener consistencia
                    const result = await processBatch(hourRecords, type, saveLocally, hourKey); // Pasar hourKey
                    if (result) {
                        filesCreated++;
                        totalRecords += result.count;
                        // Log de progreso cada 100 ficheros
                        if (filesCreated > 0 && filesCreated % 100 === 0) {
                            Logger.info(`Progreso: ${filesCreated} archivos creados, ${totalRecords} registros procesados en total${formatOverallDateForLog()}`);
                        }
                    }
                }
            } else {
                // Usar batchSize
                for (let i = 0; i < processedRecords.length; i += effectiveBatchSize) {
                    const batch = processedRecords.slice(i, i + effectiveBatchSize);
                    // Actualizar overallLastProcessedDate con el último registro de este batch específico
                    if (batch.length > 0 && batch[batch.length - 1].created_at) {
                        overallLastProcessedDate = batch[batch.length - 1].created_at;
                    }
                    Logger.info(`Procesando lote de ${batch.length} registros (${i + 1}-${i + batch.length} de ${processedRecords.length})${formatOverallDateForLog()}`);

                    // Procesar el lote
                    const result = await processBatch(batch, type, saveLocally);
                    if (result) {
                        filesCreated++;
                        totalRecords += result.count;
                        // Log de progreso cada 100 ficheros
                        if (filesCreated > 0 && filesCreated % 100 === 0) {
                            Logger.info(`Progreso: ${filesCreated} archivos creados, ${totalRecords} registros procesados en total${formatOverallDateForLog()}`);
                        }
                    }
                }
            }
        } else {
            // Modo normal o test-db: obtener datos de MySQL
            connection = await connectToMySQL();
            // Pasar 'id' a buildQuery
            const { query, params } = buildQuery(queryNumber, startDate, endDate, limit, id);

            if (testDb) {
                Logger.info(`Modo TEST-DB: Obteniendo datos de MySQL y guardando localmente`);
            }

            // Ejecutar la consulta y procesar los resultados
            Logger.info(`Ejecutando consulta: ${query}`);
            Logger.debug(`Parámetros de la consulta: ${JSON.stringify(params)}`);

            // Ejecutar la consulta como stream para procesar los resultados a medida que llegan
            Logger.info(`Ejecutando consulta como stream para procesar registros a medida que llegan`);

            // Crear una promesa para manejar la finalización del stream
            await new Promise((resolve, reject) => {
                // Estructuras para almacenar registros temporalmente
                const recordsByHour = new Map(); // Cambiado de recordsByMinute
                const records = [];
                let count = 0;
                let currentHourKey = null; // Cambiado de currentMinuteKey

                // Crear un stream de la consulta
                const queryStream = connection.query(query, params);

                // Evento para cada fila
                queryStream.on('result', async (row) => {
                    try {
                        count++;
                        // Procesar el registro
                        const processedRecord = processRecord(row, type);
                        // Actualizar la fecha del último registro procesado
                        if (processedRecord.created_at) {
                            overallLastProcessedDate = processedRecord.created_at;
                        }

                        if (count % 10000 === 0) {
                            Logger.info(`Procesados ${count} registros${formatOverallDateForLog()}`);
                        }

                        // Si se usa agrupación por hora
                        if (effectiveBatchSize === 0) {
                            const hourKey = getHourKey(processedRecord); // Usar getHourKey

                            // Si es una nueva hora y ya tenemos registros de la hora anterior, procesarlos
                            if (currentHourKey && hourKey !== currentHourKey && recordsByHour.has(currentHourKey)) {
                                // Pausar el stream mientras procesamos el lote
                                connection.pause();

                                const hourRecords = recordsByHour.get(currentHourKey);
                                // Actualizar overallLastProcessedDate con el último registro de este grupo horario
                                if (hourRecords.length > 0 && hourRecords[hourRecords.length - 1].created_at) {
                                    overallLastProcessedDate = hourRecords[hourRecords.length - 1].created_at;
                                }
                                Logger.info(`Procesando lote de ${hourRecords.length} registros de la hora ${currentHourKey}${formatOverallDateForLog()}`);

                                try {
                                    // Procesar el lote pasando la clave de la hora para mantener consistencia
                                    const result = await processBatch(hourRecords, type, saveLocally, currentHourKey); // Pasar currentHourKey
                                    if (result) {
                                        filesCreated++;
                                        totalRecords += result.count;
                                        // Log de progreso cada 100 ficheros
                                        if (filesCreated > 0 && filesCreated % 100 === 0) {
                                            Logger.info(`Progreso: ${filesCreated} archivos creados, ${totalRecords} registros procesados en total${formatOverallDateForLog()}`);
                                        }
                                    }

                                    // Limpiar la hora procesada para liberar memoria
                                    recordsByHour.delete(currentHourKey);

                                    // Reanudar el stream
                                    connection.resume();
                                } catch (error) {
                                    Logger.error('Error al procesar lote:', error); // Modificado
                                    reject(error);
                                }
                            }

                            // Guardar el registro en el mapa de horas
                            if (!recordsByHour.has(hourKey)) {
                                recordsByHour.set(hourKey, []);
                            }
                            recordsByHour.get(hourKey).push(processedRecord);
                            currentHourKey = hourKey;
                        } else {
                            // Usar batchSize
                            records.push(processedRecord);
                            // Actualizar overallLastProcessedDate con el último registro de este batch si es necesario
                            if (records.length > 0 && records[records.length-1].created_at) {
                                overallLastProcessedDate = records[records.length-1].created_at;
                            }


                            // Si se alcanzó el tamaño del lote, procesarlo
                            if (records.length >= effectiveBatchSize) {
                                // Pausar el stream mientras procesamos el lote
                                connection.pause();

                                Logger.info(`Procesando lote de ${records.length} registros${formatOverallDateForLog()}`);

                                try {
                                    // Procesar el lote
                                    const result = await processBatch(records, type, saveLocally);
                                    if (result) {
                                        filesCreated++;
                                        totalRecords += result.count;
                                        // Log de progreso cada 100 ficheros
                                        if (filesCreated > 0 && filesCreated % 100 === 0) {
                                            Logger.info(`Progreso: ${filesCreated} archivos creados, ${totalRecords} registros procesados en total${formatOverallDateForLog()}`);
                                        }
                                    }

                                    // Limpiar el array para el siguiente lote
                                    records.length = 0;

                                    // Reanudar el stream
                                    connection.resume();
                                } catch (error) {
                                    Logger.error('Error al procesar lote:', error); // Modificado
                                    reject(error);
                                }
                            }
                        }
                    } catch (error) {
                        Logger.error('Error al procesar registro:', error); // Modificado
                        reject(error);
                    }
                });

                // Evento para errores
                queryStream.on('error', (error) => {
                    Logger.error('Error en el stream de la consulta:', error); // Modificado
                    reject(error);
                });

                // Evento para el fin del stream
                queryStream.on('end', async () => {
                    Logger.info(`Stream de consulta finalizado. Procesados ${count} registros en total${formatOverallDateForLog()}.`);

                    try {
                        // Procesar los registros restantes
                        if (effectiveBatchSize === 0) {
                            // Procesar los registros restantes agrupados por hora
                            for (const [hourKey, hourRecords] of recordsByHour.entries()) { // Cambiado a recordsByHour y hourKey
                                if (hourRecords.length > 0) {
                                    // Actualizar overallLastProcessedDate con el último registro de este grupo horario
                                    if (hourRecords.length > 0 && hourRecords[hourRecords.length - 1].created_at) {
                                        overallLastProcessedDate = hourRecords[hourRecords.length - 1].created_at;
                                    }
                                    Logger.info(`Procesando lote final de ${hourRecords.length} registros de la hora ${hourKey}${formatOverallDateForLog()}`);

                                    // Procesar el lote pasando la clave de la hora para mantener consistencia
                                    const result = await processBatch(hourRecords, type, saveLocally, hourKey); // Pasar hourKey
                                    if (result) {
                                        filesCreated++;
                                        totalRecords += result.count;
                                        // Log de progreso cada 100 ficheros (aunque es menos probable aquí, por consistencia)
                                        if (filesCreated > 0 && filesCreated % 100 === 0) {
                                            Logger.info(`Progreso: ${filesCreated} archivos creados, ${totalRecords} registros procesados en total${formatOverallDateForLog()}`);
                                        }
                                    }
                                }
                            }
                        } else if (records.length > 0) {
                            // Procesar los registros restantes cuando se usa batchSize
                             if (records.length > 0 && records[records.length-1].created_at) {
                                overallLastProcessedDate = records[records.length-1].created_at;
                            }
                            Logger.info(`Procesando lote final de ${records.length} registros${formatOverallDateForLog()}`);

                            // Procesar el lote
                            const result = await processBatch(records, type, saveLocally);
                            if (result) {
                                filesCreated++;
                                totalRecords += result.count;
                                 // Log de progreso cada 100 ficheros
                                if (filesCreated > 0 && filesCreated % 100 === 0) {
                                    Logger.info(`Progreso: ${filesCreated} archivos creados, ${totalRecords} registros procesados en total${formatOverallDateForLog()}`);
                                }
                            }
                        }

                        resolve();
                    } catch (error) {
                        Logger.error('Error al procesar lotes finales:', error); // Modificado
                        reject(error);
                    }
                });
            });

            // Cerrar la conexión a MySQL
            await connection.end();
            Logger.info('Conexión a MySQL cerrada');
        }

        // Verificar si se procesaron datos
        if (totalRecords === 0) {
            Logger.warn('No se encontraron datos para procesar');
            return {
                statusCode: 200,
                body: 'No se encontraron datos para procesar'
            };
        }

        let successMessage;
        if (test) {
            successMessage = `Proceso en modo TEST completado exitosamente. Se generaron ${totalRecords} registros en ${filesCreated} archivos.${formatOverallDateForLog()}`;
        } else if (testDb) {
            successMessage = `Proceso en modo TEST-DB completado exitosamente. Se obtuvieron ${totalRecords} registros de MySQL y se guardaron en ${filesCreated} archivos locales.${formatOverallDateForLog()}`;
        } else {
            successMessage = `Proceso completado exitosamente. Se procesaron ${totalRecords} registros en ${filesCreated} archivos.${formatOverallDateForLog()}`;
        }

        Logger.info(successMessage);

        return {
            statusCode: 200,
            body: successMessage
        };
    } catch (error) {
        Logger.error('Error en el proceso principal:', error); // Modificado

        // Cerrar la conexión a MySQL si está abierta
        if (connection) {
            try {
                await connection.end();
                Logger.info('Conexión a MySQL cerrada después de error');
            } catch (closeError) {
                Logger.error('Error al cerrar la conexión a MySQL:', closeError); // Modificado
            }
        }

        return {
            statusCode: 500,
            body: `Error en el proceso: ${error.message}`
        };
    }
};

/**
 * Función para ejecutar el script desde la línea de comandos
 */
const runFromCommandLine = async () => {
    try {
        // Obtener argumentos de la línea de comandos
        const options = {};

        // Procesar argumentos
        for (let i = 2; i < process.argv.length; i++) {
            const arg = process.argv[i];

            if (arg === '--test') {
                options.test = true;
            } else if (arg.startsWith('--query=')) {
                options.queryNumber = arg.substring(8);
            } else if (arg === '--test-db') {
                options.testDb = true;
            } else if (arg.startsWith('--start=')) {
                options.startDate = arg.substring(8);
            } else if (arg.startsWith('--end=')) {
                options.endDate = arg.substring(6);
            } else if (arg.startsWith('--limit=')) {
                options.limit = parseInt(arg.substring(8));
            } else if (arg.startsWith('--batch-size=')) {
                options.batchSize = parseInt(arg.substring(13));
            } else if (arg.startsWith('--id=')) { // Añadido el parseo para --id
                options.id = arg.substring(5);
            } else {
                Logger.warn(`Opción desconocida: ${arg}`);
            }
        }

        Logger.info(`Ejecutando con opciones: ${JSON.stringify(options)}`);

        // Ejecutar la función principal
        const result = await main(options);
        // No necesitamos registrar el mensaje de nuevo, ya se registró en la función main
        process.exit(0);
    } catch (error) {
        Logger.error('Error en runFromCommandLine:', error); // Modificado para pasar el objeto error completo
        process.exit(1);
    }
};

// Si el script se ejecuta directamente (no se importa como módulo)
if (process.argv[1] === fileURLToPath(import.meta.url)) {
    runFromCommandLine();
}
