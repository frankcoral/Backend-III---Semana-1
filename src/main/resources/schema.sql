CREATE TABLE IF NOT EXISTS transacciones_procesadas (
    id BIGINT PRIMARY KEY,
    fecha DATE,
    monto DECIMAL(15,2),
    tipo VARCHAR(50),
    anomala BOOLEAN NOT NULL,
    motivo_anomalia VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS resumen_transacciones (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    total_procesadas INT NOT NULL,
    total_validas INT NOT NULL,
    total_anomalas INT NOT NULL,
    total_creditos DECIMAL(15,2) NOT NULL,
    total_debitos DECIMAL(15,2) NOT NULL,
    fecha_ejecucion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS intereses_procesados (
    cuenta_id BIGINT PRIMARY KEY,
    nombre VARCHAR(100),
    saldo_inicial DECIMAL(15,2),
    edad INT,
    tipo VARCHAR(50),
    tasa_interes DECIMAL(10,4),
    interes_calculado DECIMAL(15,2),
    saldo_final DECIMAL(15,2),
    valida BOOLEAN NOT NULL,
    motivo_error VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS resumen_intereses (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    total_procesadas INT NOT NULL,
    total_validas INT NOT NULL,
    total_invalidas INT NOT NULL,
    interes_total DECIMAL(15,2) NOT NULL,
    fecha_ejecucion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS movimientos_anuales (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    cuenta_id BIGINT NOT NULL,
    fecha DATE,
    transaccion VARCHAR(50),
    monto DECIMAL(15,2),
    descripcion VARCHAR(255),
    valido BOOLEAN NOT NULL,
    motivo_error VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS estados_cuenta_anuales (
    cuenta_id BIGINT PRIMARY KEY,
    total_movimientos INT NOT NULL,
    movimientos_validos INT NOT NULL,
    movimientos_invalidos INT NOT NULL,
    total_depositos DECIMAL(15,2) NOT NULL,
    total_cargos DECIMAL(15,2) NOT NULL,
    saldo_neto DECIMAL(15,2) NOT NULL,
    fecha_generacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);