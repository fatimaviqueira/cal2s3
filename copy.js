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

// Configuración del entorno
const ENVIRONMENT = process.env.ENVIRONMENT || 'development';
const BUCKET_NAME = process.env.BUCKET_NAME || `lam-${ENVIRONMENT}-teixo`;
const AWS_REGION = process.env.AWS_REGION || 'eu-west-1';
const LOG_LEVEL = process.env.LOG_LEVEL || 'info';

// Configuración de MySQL
const MYSQL_CONFIG = {
    host: process.env.MYSQL_HOST || 'localhost',
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
    
    debug: function(message) {
        if (this.currentLevel <= LogLevels.DEBUG) {
            console.log(`[DEBUG] ${message}`);
        }
    },
    
    info: function(message) {
        if (this.currentLevel <= LogLevels.INFO) {
            console.log(`[INFO] ${message}`);
        }
    },
    
    warn: function(message) {
        if (this.currentLevel <= LogLevels.WARN) {
            console.warn(`[WARN] ${message}`);
        }
    },
    
    error: function(message) {
        if (this.currentLevel <= LogLevels.ERROR) {
            console.error(`[ERROR] ${message}`);
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
 * Genera un nombre de archivo basado en una fecha y un tipo
 * @param {string} type - Tipo de datos (usado como prefijo en el nombre del archivo)
 * @param {Date} date - Fecha a utilizar para el nombre del archivo
 * @returns {string} Ruta completa del archivo incluyendo carpetas organizadas por fecha
 */
const getTimestampFilename = (type, date) => {
    // Descomponer la fecha y hora en sus componentes
    const { year, month, day, hours, minutes, seconds, milliseconds } = decomposeDate(date);
    // Crear el nombre del archivo con la fecha y hora
    return `${year}/${month}/${day}/${hours}/${type}_${year}${month}${day}_${hours}${minutes}_${seconds}_${milliseconds}_${uuidv4()}.json.gz`;
};

/**
 * Genera un nombre de archivo basado en la marca de tiempo actual y un tipo
 * @param {string} type - Tipo de datos (usado como prefijo en el nombre del archivo)
 * @returns {string} Ruta completa del archivo incluyendo carpetas organizadas por fecha
 */
const getCurrentTimestampFilename = (type) => {
    // Obtener la fecha y hora actuales
    const now = new Date();
    return getTimestampFilename(type, now);
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
        Logger.error(`Error al conectar a MySQL: ${error}`);
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
 * @param {string} minuteKey - Clave del minuto para agrupar registros (opcional)
 * @returns {Promise<Object>} Información sobre el lote procesado
 */
const processBatch = async (records, type, saveLocally, minuteKey = null) => {
    if (records.length === 0) return null;
    
    // Obtener la fecha del primer registro para el nombre del archivo
    const firstRecord = records[0];
    const firstDate = new Date(firstRecord.created_at);
    
    // Generar el nombre del archivo basado en el intervalo de 10 minutos
    // Si se proporciona minuteKey, usarla para generar un nombre consistente para el mismo intervalo de 10 minutos
    let fileName;
    if (minuteKey) {
        // Extraer componentes de la clave del minuto (año-mes-día-hora-minuto)
        const [year, month, day, hour, minute] = minuteKey.split('-');
        // Crear una fecha con estos componentes
        const minuteDate = new Date(year, month - 1, day, hour, minute);
        fileName = `${type}/${getTimestampFilename(type, minuteDate)}`;
    } else {
        fileName = `${type}/${getTimestampFilename(type, firstDate)}`;
    }
    
    // Añadir el campo s3file a todos los registros con la ruta completa
    const fullPath = `${BUCKET_NAME}/${fileName}`;
    for (const record of records) {
        record.s3file = fullPath;
    }
    
    // Convertir los registros acumulados a JSON
    const jsonContent = JSON.stringify(records, null, 2);
    
    // Comprimir el contenido
    const compressedContent = zlib.gzipSync(jsonContent, { level: 9 });
    Logger.info(`Contenido comprimido, tamaño: ${compressedContent.length} bytes`);
    
    // Guardar el contenido comprimido
    if (saveLocally) {
        await saveToLocalFile(compressedContent, fileName);
    } else {
        await saveToS3(compressedContent, fileName);
    }
    
    return {
        count: records.length,
        date: firstDate,
        fileName
    };
};

/**
 * Agrupa registros por intervalos de 10 minutos
 * @param {Object} record - Registro a agrupar
 * @returns {string} Clave del minuto para agrupar
 */
const getMinuteKey = (record) => {
    const createdAt = new Date(record.created_at);
    // Redondear los minutos al múltiplo de 10 más cercano
    const minutes = createdAt.getMinutes();
    const roundedMinutes = Math.floor(minutes / 10) * 10;
    return `${createdAt.getFullYear()}-${createdAt.getMonth() + 1}-${createdAt.getDate()}-${createdAt.getHours()}-${roundedMinutes}`;
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
        Logger.error(`Error al guardar el archivo en S3: ${error}`);
        throw error;
    }
};

/**
 * Construye la consulta SQL con filtros de fecha
 * @param {string} startDate - Fecha de inicio (YYYY-MM-DD)
 * @param {string} endDate - Fecha de fin (YYYY-MM-DD)
 * @param {number} limit - Límite de registros
 * @returns {Object} Objeto con la consulta y los parámetros
 */
const buildQuery = (startDate, endDate, limit) => {
    // Consulta base para cada tabla
    const baseSelect = `
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
            cal.request_trace_id,
            cal.error_message,
            cal.server_instance_name,
            acc.client_code as account_client_code,
            acc.client_name as account_client_name,
            acc.client_type as account_client_type,
            acc.description as account_description
    `;
    
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
        -- WHERE cal.id <= 1040103545
        WHERE cal.id = 1094730020
    `;
    
    const params = [];
    
    // Añadir filtro de fecha de inicio si se especifica
    if (startDate) {
        query3 += ` AND cal.created_at >= ?`;
        params.push(startDate);
    }
    
    // Añadir filtro de fecha de fin si se especifica
    if (endDate) {
        query3 += ` AND cal.created_at <= ?`;
        params.push(endDate);
    }
    
    // Combinar las consultas con UNION ALL
    let query = query3; // UNION ALL (${query2}) UNION ALL (${query3})`;
    
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
        Logger.info(`Guardando archivo local: ${fileName}`);
        await fs.writeFile(fileName, content);
        Logger.info(`Archivo guardado en ${fileName}`);
    } catch (error) {
        Logger.error(`Error al guardar el archivo local: ${error}`);
        throw error;
    }
};

/**
 * Función principal que ejecuta todo el proceso
 * @param {Object} options - Opciones de ejecución
 * @param {string} options.startDate - Fecha de inicio (YYYY-MM-DD)
 * @param {string} options.endDate - Fecha de fin (YYYY-MM-DD)
 * @param {number} options.limit - Límite de registros a procesar
 * @param {boolean} options.test - Modo test (genera datos aleatorios)
 * @param {boolean} options.testDb - Modo test-db (usa MySQL pero guarda localmente)
 * @param {number} options.batchSize - Tamaño del lote de registros por archivo (0 para un solo archivo)
 * @returns {Promise<Object>} Resultado de la operación
 */
export const main = async (options = {}) => {
    const { startDate, endDate, limit, test = false, testDb = false, batchSize = 0 } = options;
    
    // Determinar si se guardarán los archivos localmente
    const saveLocally = test || testDb;
    
    Logger.info(`Iniciando proceso con opciones: ${JSON.stringify(options)}`);
    let connection;
    let filesCreated = 0;
    let totalRecords = 0;
    
    try {
        // Generar nombre de archivo base (siempre de tipo 'cal')
        const type = 'cal';
        
        // Si no se especificó un tamaño de lote, usamos un valor predeterminado para evitar problemas de memoria
        const effectiveBatchSize = batchSize || 0; // 0 significa agrupar por intervalos de 10 minutos
        Logger.info(`Tamaño de lote efectivo: ${effectiveBatchSize || 'agrupación por intervalos de 10 minutos'}`);
        
        if (test) {
            // Modo test: generar datos aleatorios
            const testLimit = limit || 100;
            Logger.info(`Modo TEST: Generando ${testLimit} registros aleatorios`);
            
            // Generar datos aleatorios
            const randomObjects = generateRandomObjects(testLimit);
            Logger.info(`Generados ${randomObjects.length} registros aleatorios`);
            
            // Procesar los registros
            const processedRecords = randomObjects.map(record => processRecord(record, type));
            
            // Si se usa agrupación por minuto
            if (effectiveBatchSize === 0) {
                // Agrupar por minuto
                const recordsByMinute = new Map();
                
                // Agrupar registros por minuto
                for (const record of processedRecords) {
                    const minuteKey = getMinuteKey(record);
                    if (!recordsByMinute.has(minuteKey)) {
                        recordsByMinute.set(minuteKey, []);
                    }
                    recordsByMinute.get(minuteKey).push(record);
                }
                
                // Procesar cada grupo de minutos
                for (const [minuteKey, minuteRecords] of recordsByMinute.entries()) {
                    Logger.info(`Procesando lote de ${minuteRecords.length} registros del minuto ${minuteKey}`);
                    
                    // Procesar el lote pasando la clave del minuto para mantener consistencia
                    const result = await processBatch(minuteRecords, type, saveLocally, minuteKey);
                    if (result) {
                        filesCreated++;
                        totalRecords += result.count;
                    }
                }
            } else {
                // Usar batchSize
                for (let i = 0; i < processedRecords.length; i += effectiveBatchSize) {
                    const batch = processedRecords.slice(i, i + effectiveBatchSize);
                    Logger.info(`Procesando lote de ${batch.length} registros (${i + 1}-${i + batch.length} de ${processedRecords.length})`);
                    
                    // Procesar el lote
                    const result = await processBatch(batch, type, saveLocally);
                    if (result) {
                        filesCreated++;
                        totalRecords += result.count;
                    }
                }
            }
        } else {
            // Modo normal o test-db: obtener datos de MySQL
            connection = await connectToMySQL();
            const { query, params } = buildQuery(startDate, endDate, limit);
            
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
                const recordsByMinute = new Map();
                const records = [];
                let count = 0;
                let currentMinuteKey = null;
                
                // Crear un stream de la consulta
                const queryStream = connection.query(query, params);
                
                // Evento para cada fila
                queryStream.on('result', async (row) => {
                    try {
                        count++;
                        if (count % 1000 === 0) {
                            Logger.info(`Procesados ${count} registros`);
                        }
                        
                        // Procesar el registro
                        const processedRecord = processRecord(row, type);
                        
                        // Si se usa agrupación por intervalos de 10 minutos
                        if (effectiveBatchSize === 0) {
                            const minuteKey = getMinuteKey(processedRecord);
                            
                            // Si es un nuevo intervalo de 10 minutos y ya tenemos registros del intervalo anterior, procesarlos
                            if (currentMinuteKey && minuteKey !== currentMinuteKey && recordsByMinute.has(currentMinuteKey)) {
                                // Pausar el stream mientras procesamos el lote
                                connection.pause();
                                
                                const minuteRecords = recordsByMinute.get(currentMinuteKey);
                                Logger.info(`Procesando lote de ${minuteRecords.length} registros del intervalo ${currentMinuteKey}`);
                                
                                try {
                                    // Procesar el lote pasando la clave del minuto para mantener consistencia
                                    const result = await processBatch(minuteRecords, type, saveLocally, currentMinuteKey);
                                    if (result) {
                                        filesCreated++;
                                        totalRecords += result.count;
                                    }
                                    
                                    // Limpiar el intervalo procesado para liberar memoria
                                    recordsByMinute.delete(currentMinuteKey);
                                    
                                    // Reanudar el stream
                                    connection.resume();
                                } catch (error) {
                                    Logger.error(`Error al procesar lote: ${error}`);
                                    reject(error);
                                }
                            }
                            
                            // Guardar el registro en el mapa de minutos
                            if (!recordsByMinute.has(minuteKey)) {
                                recordsByMinute.set(minuteKey, []);
                            }
                            recordsByMinute.get(minuteKey).push(processedRecord);
                            currentMinuteKey = minuteKey;
                        } else {
                            // Usar batchSize
                            records.push(processedRecord);
                            
                            // Si se alcanzó el tamaño del lote, procesarlo
                            if (records.length >= effectiveBatchSize) {
                                // Pausar el stream mientras procesamos el lote
                                connection.pause();
                                
                                Logger.info(`Procesando lote de ${records.length} registros`);
                                
                                try {
                                    // Procesar el lote
                                    const result = await processBatch(records, type, saveLocally);
                                    if (result) {
                                        filesCreated++;
                                        totalRecords += result.count;
                                    }
                                    
                                    // Limpiar el array para el siguiente lote
                                    records.length = 0;
                                    
                                    // Reanudar el stream
                                    connection.resume();
                                } catch (error) {
                                    Logger.error(`Error al procesar lote: ${error}`);
                                    reject(error);
                                }
                            }
                        }
                    } catch (error) {
                        Logger.error(`Error al procesar registro: ${error}`);
                        reject(error);
                    }
                });
                
                // Evento para errores
                queryStream.on('error', (error) => {
                    Logger.error(`Error en el stream de la consulta: ${error}`);
                    reject(error);
                });
                
                // Evento para el fin del stream
                queryStream.on('end', async () => {
                    Logger.info(`Stream de consulta finalizado. Procesados ${count} registros en total.`);
                    
                    try {
                        // Procesar los registros restantes
                        if (effectiveBatchSize === 0) {
                            // Procesar los registros restantes agrupados por intervalos de 10 minutos
                            for (const [minuteKey, minuteRecords] of recordsByMinute.entries()) {
                                if (minuteRecords.length > 0) {
                                    Logger.info(`Procesando lote final de ${minuteRecords.length} registros del intervalo ${minuteKey}`);
                                    
                                    // Procesar el lote pasando la clave del minuto para mantener consistencia
                                    const result = await processBatch(minuteRecords, type, saveLocally, minuteKey);
                                    if (result) {
                                        filesCreated++;
                                        totalRecords += result.count;
                                    }
                                }
                            }
                        } else if (records.length > 0) {
                            // Procesar los registros restantes cuando se usa batchSize
                            Logger.info(`Procesando lote final de ${records.length} registros`);
                            
                            // Procesar el lote
                            const result = await processBatch(records, type, saveLocally);
                            if (result) {
                                filesCreated++;
                                totalRecords += result.count;
                            }
                        }
                        
                        resolve();
                    } catch (error) {
                        Logger.error(`Error al procesar lotes finales: ${error}`);
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
            successMessage = `Proceso en modo TEST completado exitosamente. Se generaron ${totalRecords} registros en ${filesCreated} archivos.`;
        } else if (testDb) {
            successMessage = `Proceso en modo TEST-DB completado exitosamente. Se obtuvieron ${totalRecords} registros de MySQL y se guardaron en ${filesCreated} archivos locales.`;
        } else {
            successMessage = `Proceso completado exitosamente. Se procesaron ${totalRecords} registros en ${filesCreated} archivos.`;
        }
        
        Logger.info(successMessage);
        
        return {
            statusCode: 200,
            body: successMessage
        };
    } catch (error) {
        Logger.error(`Error en el proceso principal: ${error}`);
        
        // Cerrar la conexión a MySQL si está abierta
        if (connection) {
            try {
                await connection.end();
                Logger.info('Conexión a MySQL cerrada después de error');
            } catch (closeError) {
                Logger.error(`Error al cerrar la conexión a MySQL: ${closeError}`);
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
            } else if (arg === '--test-db') {
                options.testDb = true;
            } else if (arg.startsWith('--start=')) {
                options.startDate = arg.substring(8);
            } else if (arg.startsWith('--end=')) {
                options.endDate = arg.substring(6);
            } else if (arg === '--start' && i + 1 < process.argv.length) {
                options.startDate = process.argv[++i];
            } else if (arg === '--end' && i + 1 < process.argv.length) {
                options.endDate = process.argv[++i];
            } else if (arg.startsWith('--limit=')) {
                options.limit = parseInt(arg.substring(8));
            } else if (arg === '--limit' && i + 1 < process.argv.length) {
                options.limit = parseInt(process.argv[++i]);
            } else if (arg.startsWith('--batch-size=')) {
                options.batchSize = parseInt(arg.substring(13));
            } else if (arg === '--batch-size' && i + 1 < process.argv.length) {
                options.batchSize = parseInt(process.argv[++i]);
            }
        }
        
        Logger.info(`Ejecutando con opciones: ${JSON.stringify(options)}`);
        
        // Ejecutar la función principal
        const result = await main(options);
        // No necesitamos registrar el mensaje de nuevo, ya se registró en la función main
        process.exit(0);
    } catch (error) {
        Logger.error(`Error: ${error.message}`);
        process.exit(1);
    }
};

// Si el script se ejecuta directamente (no se importa como módulo)
if (process.argv[1] === fileURLToPath(import.meta.url)) {
    runFromCommandLine();
}
