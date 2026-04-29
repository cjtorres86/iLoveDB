"""
================================================================================
ILOVEDB - HR DATA UNIFIER
================================================================================
Sistema de integración y consolidación de nóminas de Recursos Humanos
Versión: 1.5 (Múltiples formatos de descarga + Dropdown en botón Descargar)
Autor: iLoveDB
================================================================================
"""

# =============================================================================
# IMPORTS
# =============================================================================
from fastapi import FastAPI, UploadFile, File, HTTPException, Request
from fastapi.responses import StreamingResponse, HTMLResponse
import pandas as pd
import io
import unicodedata
import re
from datetime import date, datetime

# =============================================================================
# CONFIGURACIÓN INICIAL
# =============================================================================
app = FastAPI(
    title="iLoveDB - HR Data Unifier",
    description="API para unificación de nóminas de RRHH"
)
dataframes_crudos = []  # Almacena los DataFrames normalizados (sin fusionar aún)

# =============================================================================
# FUNCIONES AUXILIARES
# =============================================================================

def quitar_tildes(texto):
    if not isinstance(texto, str):
        return texto
    return ''.join(
        c for c in unicodedata.normalize('NFD', texto)
        if unicodedata.category(c) != 'Mn'
    )

def normalizar_texto(texto):
    if pd.isna(texto):
        return ""
    texto = str(texto).strip().lower()
    texto = quitar_tildes(texto)
    texto = re.sub(r'\s+', ' ', texto)
    return texto

def normalizar_rut_chileno(rut):
    if pd.isna(rut):
        return None
    rut_str = str(rut).strip()
    try:
        if isinstance(rut, float):
            rut_str = str(int(rut))
    except:
        pass
    limpio = re.sub(r'[\.\-/]', '', rut_str)
    numeros = re.findall(r'\d+', limpio)
    if not numeros:
        return None
    todo_junto = ''.join(numeros)
    if len(todo_junto) == 8:
        cuerpo = todo_junto[:-1]
        dv = todo_junto[-1]
    elif len(todo_junto) == 9:
        cuerpo = todo_junto[:-1]
        dv = todo_junto[-1]
    elif len(todo_junto) == 7:
        cuerpo = todo_junto[:-1]
        dv = todo_junto[-1]
    else:
        return None
    if len(cuerpo) < 6 or len(cuerpo) > 8:
        return None
    return f"{cuerpo}-{dv}"

def calcular_edad(fecha_nac):
    """Calcula la edad en años a partir de la fecha de nacimiento"""
    if fecha_nac is None:
        return None
    try:
        if pd.isna(fecha_nac):
            return None
        hoy = date.today()
        return hoy.year - fecha_nac.year - ((hoy.month, hoy.day) < (fecha_nac.month, fecha_nac.day))
    except Exception:
        return None

def calcular_antiguedad(fecha_ingreso):
    """Calcula la antigüedad en años a partir de la fecha de ingreso"""
    if fecha_ingreso is None:
        return None
    try:
        if pd.isna(fecha_ingreso):
            return None
        hoy = date.today()
        return hoy.year - fecha_ingreso.year - ((hoy.month, hoy.day) < (fecha_ingreso.month, fecha_ingreso.day))
    except Exception:
        return None

def combinar_valores(series):
    valores_no_nulos = series.dropna()
    return valores_no_nulos.iloc[0] if len(valores_no_nulos) > 0 else None

def rellenar_vacios(df):
    df = df.fillna("Sin Información")
    df = df.replace([None], "Sin Información")
    return df

def resolver_columnas_duplicadas(df):
    """
    Renombra columnas duplicadas agregando un sufijo _1, _2, etc.
    Preserva toda la información, no elimina datos.
    """
    cols = pd.Series(df.columns)
    for dup in cols[cols.duplicated()].unique():
        indices = cols[cols == dup].index.tolist()
        for i, idx in enumerate(indices, 1):
            cols.iloc[idx] = f"{dup}_{i}"
    df.columns = cols
    return df

# =============================================================================
# FUNCIONES PARA DETECCIÓN DE ARCHIVOS CON/SIN RUT
# =============================================================================

def tiene_rut_real(df):
    if "rut" not in df.columns:
        return False
    ruts = df["rut"].dropna()
    if len(ruts) == 0:
        return False
    for rut in ruts:
        rut_str = str(rut)
        if not rut_str.startswith("TEMP_") and re.match(r'^\d{7,8}-\d$', rut_str):
            return True
    return False

def tiene_info_nombre(df):
    columnas_nombre = ["nombre", "apellido_paterno", "apellido_materno"]
    for col in columnas_nombre:
        if col in df.columns and df[col].notna().sum() > 0:
            return True
    return False

# =============================================================================
# FUNCIONES PARA JERARQUÍA DE APELLIDOS
# =============================================================================

def generar_claves_busqueda(row):
    return {
        "apellido_paterno": normalizar_texto(row.get("apellido_paterno", "")),
        "apellido_materno": normalizar_texto(row.get("apellido_materno", "")),
        "nombre": normalizar_texto(row.get("nombre", "")),
    }

def generar_rut_temporal(row):
    claves = generar_claves_busqueda(row)
    partes = []
    if claves["apellido_paterno"]:
        partes.append(claves["apellido_paterno"])
    if claves["apellido_materno"]:
        partes.append(claves["apellido_materno"])
    if claves["nombre"]:
        partes.append(claves["nombre"])
    if not partes:
        return "TEMP_DESCONOCIDO"
    return f"TEMP_{'_'.join(partes)}"

def asociar_por_jerarquia(df_sin_rut, df_base):
    if df_base.empty:
        df_sin_rut["rut_asociado"] = None
        return df_sin_rut
    df_base = df_base.copy()
    df_base["_claves"] = df_base.apply(generar_claves_busqueda, axis=1)
    
    def encontrar_rut(row):
        claves = generar_claves_busqueda(row)
        candidatos = df_base[
            df_base["_claves"].apply(lambda x: x["apellido_paterno"] == claves["apellido_paterno"] and x["apellido_paterno"] != "")
        ]
        if len(candidatos) > 1 and claves["apellido_materno"]:
            candidatos = candidatos[
                candidatos["_claves"].apply(lambda x: x["apellido_materno"] == claves["apellido_materno"])
            ]
        if len(candidatos) > 1 and claves["nombre"]:
            candidatos = candidatos[
                candidatos["_claves"].apply(lambda x: x["nombre"] == claves["nombre"])
            ]
        if len(candidatos) == 1:
            return candidatos.iloc[0]["rut"]
        return None
    
    df_sin_rut["rut_asociado"] = df_sin_rut.apply(encontrar_rut, axis=1)
    return df_sin_rut

def fusionar_por_rut(dataframes_list):
    if not dataframes_list:
        return None
    
    # Resolver columnas duplicadas en cada DataFrame
    dataframes_limpios = []
    for df in dataframes_list:
        df_copy = df.copy()
        df_copy = resolver_columnas_duplicadas(df_copy)
        dataframes_limpios.append(df_copy)
    
    # Unificar todos los DataFrames
    df_concatenado = pd.concat(dataframes_limpios, ignore_index=True, join='outer')
    
    # Encontrar la columna RUT principal
    columna_rut = None
    for col in df_concatenado.columns:
        if col == "rut" or col.startswith("rut_"):
            columna_rut = col
            break
    
    if not columna_rut:
        return None
    
    # Agrupar por RUT
    df_fusionado = df_concatenado.groupby(columna_rut, as_index=False).agg(combinar_valores)
    
    # Renombrar la columna RUT a "rut"
    if columna_rut != "rut":
        df_fusionado.rename(columns={columna_rut: "rut"}, inplace=True)
    
    # Reordenar columnas
    columnas = ["rut"] + [col for col in df_fusionado.columns if col != "rut"]
    df_fusionado = df_fusionado[columnas]
    
    return df_fusionado

def fusionar_todo():
    if not dataframes_crudos:
        return None
    
    archivos_con_rut = []
    archivos_con_nombre = []
    archivos_otros = []
    
    for df in dataframes_crudos:
        if tiene_rut_real(df):
            archivos_con_rut.append(df)
        elif tiene_info_nombre(df):
            archivos_con_nombre.append(df)
        else:
            archivos_otros.append(df)
    
    df_base = fusionar_por_rut(archivos_con_rut) if archivos_con_rut else pd.DataFrame()
    
    for df_nombre in archivos_con_nombre:
        if df_base.empty:
            df_nombre["rut"] = df_nombre.apply(generar_rut_temporal, axis=1)
            df_base = df_nombre
        else:
            df_asociado = asociar_por_jerarquia(df_nombre, df_base)
            df_asociado.loc[df_asociado["rut_asociado"].notna(), "rut"] = df_asociado["rut_asociado"]
            sin_asociar = df_asociado["rut_asociado"].isna()
            df_asociado.loc[sin_asociar, "rut"] = df_asociado[sin_asociar].apply(generar_rut_temporal, axis=1)
            df_asociado.drop(columns=["rut_asociado"], inplace=True)
            
            # Resolver duplicados antes de concatenar
            df_base = resolver_columnas_duplicadas(df_base)
            df_asociado = resolver_columnas_duplicadas(df_asociado)
            
            df_base = pd.concat([df_base, df_asociado], ignore_index=True, join='outer')
            df_base = df_base.groupby("rut", as_index=False).agg(combinar_valores)
    
    if archivos_otros:
        for df_otro in archivos_otros:
            df_otro = resolver_columnas_duplicadas(df_otro)
        df_base = pd.concat([df_base] + archivos_otros, ignore_index=True, join='outer')
        if "rut" in df_base.columns:
            df_base = df_base.groupby("rut", as_index=False).agg(combinar_valores)
    
    if "rut" in df_base.columns:
        df_base = df_base.sort_values(by="rut").reset_index(drop=True)
    
    return df_base

# =============================================================================
# FUNCIONES PARA DETECCIÓN AUTOMÁTICA DE CSV/TXT
# =============================================================================

def detectar_encoding(contenido_bytes):
    encodings = ['utf-8', 'latin1', 'iso-8859-1', 'windows-1252', 'cp1252', 'ascii']
    for encoding in encodings:
        try:
            contenido_bytes.decode(encoding)
            return encoding
        except UnicodeDecodeError:
            continue
    return 'latin1'

def detectar_separador_csv(primeras_lineas):
    separadores = [',', ';', '\t', '|']
    candidatos = {}
    for sep in separadores:
        conteos = []
        for linea in primeras_lineas:
            campos = linea.split(sep)
            if len(campos) > 1:
                conteos.append(len(campos))
        if conteos:
            if len(set(conteos)) == 1:
                return sep, conteos[0]
            candidatos[sep] = max(set(conteos), key=conteos.count)
    if candidatos:
        return max(candidatos, key=lambda x: candidatos[x]), candidatos[max(candidatos, key=lambda x: candidatos[x])]
    return ',', 2

def leer_csv_automatico(contenido_bytes):
    encoding = detectar_encoding(contenido_bytes)
    texto = contenido_bytes.decode(encoding)
    lineas = [l for l in texto.split('\n')[:10] if l.strip()]
    if not lineas:
        raise ValueError("El archivo CSV está vacío")
    separador, _ = detectar_separador_csv(lineas)
    try:
        df = pd.read_csv(io.BytesIO(contenido_bytes), encoding=encoding, sep=separador, engine='python')
        return df
    except Exception as e:
        for sep in [',', ';', '\t', '|']:
            if sep == separador:
                continue
            try:
                df = pd.read_csv(io.BytesIO(contenido_bytes), encoding=encoding, sep=sep, engine='python')
                return df
            except:
                continue
        raise ValueError(f"No se pudo leer el CSV. Último error: {str(e)}")

def detectar_separador_txt(linea):
    separadores_candidatos = [',', ';', '\t', '|', ' ']
    mejor_sep = ','
    max_campos = 0
    for sep in separadores_candidatos:
        campos = linea.split(sep)
        if len(campos) > max_campos and len(campos) > 1:
            max_campos = len(campos)
            mejor_sep = sep
    return mejor_sep, max_campos

def detectar_si_tiene_cabecera(primeras_lineas, separador):
    if len(primeras_lineas) < 2:
        return False
    primera = primeras_lineas[0].split(separador)
    segunda = primeras_lineas[1].split(separador)
    if len(primera) != len(segunda):
        return False
    palabras_clave = ['rut', 'nombre', 'fecha', 'salario', 'cargo', 'id', 'apellido', 'email']
    texto_primera = ' '.join(primera).lower()
    for palabra in palabras_clave:
        if palabra in texto_primera:
            return True
    num_numeros_primera = sum(1 for campo in primera if re.match(r'^[\d\.\-]+$', campo))
    num_numeros_segunda = sum(1 for campo in segunda if re.match(r'^[\d\.\-]+$', campo))
    return num_numeros_primera < num_numeros_segunda

def parsear_clave_valor(contenido):
    lineas = contenido.strip().split('\n')
    registros = []
    registro_actual = {}
    for linea in lineas:
        linea = linea.strip()
        if not linea:
            if registro_actual:
                registros.append(registro_actual)
                registro_actual = {}
            continue
        if ':' in linea:
            partes = linea.split(':', 1)
            clave = partes[0].strip().lower()
            valor = partes[1].strip()
            registro_actual[clave] = valor
        elif '=' in linea:
            partes = linea.split('=', 1)
            clave = partes[0].strip().lower()
            valor = partes[1].strip()
            registro_actual[clave] = valor
    if registro_actual:
        registros.append(registro_actual)
    if not registros:
        raise ValueError("No se pudo parsear el formato clave:valor")
    return pd.DataFrame(registros)

def leer_txt_automatico(contenido_bytes):
    encoding = detectar_encoding(contenido_bytes)
    texto = contenido_bytes.decode(encoding)
    lineas = [l for l in texto.split('\n') if l.strip()]
    if not lineas:
        raise ValueError("El archivo TXT está vacío")
    tiene_clave_valor = False
    for linea in lineas[:5]:
        if ':' in linea or '=' in linea:
            tiene_clave_valor = True
            break
    if tiene_clave_valor:
        try:
            return parsear_clave_valor(texto)
        except Exception:
            pass
    separador, num_campos = detectar_separador_txt(lineas[0])
    tiene_cabecera = detectar_si_tiene_cabecera(lineas, separador)
    buffer = io.BytesIO(contenido_bytes)
    try:
        df = pd.read_csv(buffer, encoding=encoding, sep=separador, engine='python', header=0 if tiene_cabecera else None)
        if not tiene_cabecera:
            columnas_genericas = [f'col_{i+1}' for i in range(len(df.columns))]
            df.columns = columnas_genericas
        return df
    except Exception as e:
        raise ValueError(f"No se pudo leer el TXT. Último error: {str(e)}")

# =============================================================================
# ENDPOINTS (API)
# =============================================================================

@app.get("/")
def root():
    return {
        "mensaje": "iLoveDB - HR Data Unifier funcionando",
        "archivos_cargados": len(dataframes_crudos),
        "formato_soportado": "Excel (.xlsx, .xls), CSV (.csv) y TXT (.txt)",
        "endpoints": {
            "POST /upload": "Subir archivo Excel, CSV o TXT",
            "GET /download": "Descargar nómina unificada (formatos: xlsx, csv, txt)",
            "POST /reset": "Reiniciar todos los datos",
            "GET /stats": "Ver estadísticas",
            "GET /web": "Interfaz web"
        }
    }

@app.post("/reset")
def reset_data():
    global dataframes_crudos
    dataframes_crudos = []
    return {"mensaje": "Datos reiniciados exitosamente", "archivos_cargados": 0}

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    global dataframes_crudos
    
    if not file.filename.endswith((".xlsx", ".xls", ".csv", ".txt")):
        return {
            "error": "Formato no soportado. Aceptamos Excel (.xlsx, .xls), CSV (.csv) y TXT (.txt)",
            "archivo_enviado": file.filename
        }
    
    content = await file.read()
    try:
        if file.filename.endswith((".xlsx", ".xls")):
            df = pd.read_excel(io.BytesIO(content))
        elif file.filename.endswith(".csv"):
            df = leer_csv_automatico(content)
        elif file.filename.endswith(".txt"):
            df = leer_txt_automatico(content)
        else:
            return {"error": "Formato no soportado"}
    except Exception as e:
        return {"error": f"Error al leer archivo: {str(e)}"}
    
    # ========== NORMALIZACIÓN DE COLUMNAS ==========
    df.columns = (df.columns.str.strip().str.lower().str.replace(" ", "_").map(quitar_tildes))
    
    # ========== EQUIVALENCIAS DE COLUMNAS ==========
    columnas_equivalentes = {
        "rut_empleado": "rut", "rut_trabajador": "rut", "id": "rut", "ID": "rut",
        "id_empleado": "rut", "id_trabajador": "rut", "r.u.t": "rut", "r.u.t.": "rut",
        "run": "rut", "documento": "rut",
        "nombre_empleado": "nombre", "nombre_completo": "nombre", "nombres": "nombre",
        "empleado": "nombre", "trabajador": "nombre", "primer_nombre": "nombre",
        "apellido_paterno": "apellido_paterno", "apellido_p": "apellido_paterno",
        "paterno": "apellido_paterno", "apellido_materno": "apellido_materno",
        "apellido_m": "apellido_materno", "materno": "apellido_materno",
        "apellidos": "apellido_paterno",
        "fecha_nac": "fecha_nacimiento", "fecha_de_nacimiento": "fecha_nacimiento",
        "nacimiento": "fecha_nacimiento", "f.nac.": "fecha_nacimiento", "fechanac": "fecha_nacimiento",
        "fecha_de_ingreso": "fecha_ingreso", "ingreso": "fecha_ingreso",
        "ingreso_empresa": "fecha_ingreso", "f.ingreso": "fecha_ingreso", "fechaingreso": "fecha_ingreso",
        "jornada": "jornada_laboral", "sueldo": "remuneracion",
        "salario": "remuneracion", "sueldo_base": "remuneracion",
        "salario_base": "remuneracion", "salario_minimo": "remuneracion",
        "remuneracion_base": "remuneracion", "renta": "remuneracion",
        "zapatos": "talla_calzado",
    }
    df.rename(columns=columnas_equivalentes, inplace=True)
    
    # ========== NORMALIZAR APELLIDOS ==========
    if "apellido_paterno" in df.columns:
        df["apellido_paterno"] = df["apellido_paterno"].apply(normalizar_texto)
    if "apellido_materno" in df.columns:
        df["apellido_materno"] = df["apellido_materno"].apply(normalizar_texto)
    if "nombre" in df.columns:
        df["nombre"] = df["nombre"].apply(normalizar_texto)
    
    # ========== VALIDAR RUT O NOMBRE ==========
    if "rut" not in df.columns and not tiene_info_nombre(df):
        return {
            "error": "El archivo no contiene RUT ni información de nombre/apellidos. Columnas necesarias: 'rut' o ('nombre', 'apellido_paterno', 'apellido_materno')",
            "columnas_encontradas": df.columns.tolist()
        }
    
    total_filas = len(df)
    
    # ========== NORMALIZAR RUT ==========
    if "rut" in df.columns:
        rut_vacios = df["rut"].isna().sum()
        df = df[df["rut"].notna()]
        df = df.reset_index(drop=True)
        df["rut_normalizado"] = df["rut"].apply(normalizar_rut_chileno)
        rut_invalidos = df["rut_normalizado"].isna().sum()
        df = df[df["rut_normalizado"].notna()]
        df["rut"] = df["rut_normalizado"]
        df.drop(columns=["rut_normalizado"], inplace=True)
    else:
        rut_vacios = 0
        rut_invalidos = 0
        if "nombre" not in df.columns:
            df["nombre"] = ""
        if "apellido_paterno" not in df.columns:
            df["apellido_paterno"] = ""
        if "apellido_materno" not in df.columns:
            df["apellido_materno"] = ""
    
    # ========== CONVERTIR REMUNERACIÓN ==========
    if "remuneracion" in df.columns and len(df["remuneracion"]) > 0:
        try:
            df["remuneracion"] = pd.to_numeric(df["remuneracion"], errors="coerce")
            remuneraciones_invalidas = int(df["remuneracion"].isna().sum())
        except Exception as e:
            remuneraciones_invalidas = len(df)
            df["remuneracion"] = None
    else:
        remuneraciones_invalidas = 0
    
    # ========== NORMALIZAR FECHAS ==========
    if "fecha_ingreso" in df.columns and len(df["fecha_ingreso"]) > 0:
        try:
            df["fecha_ingreso"] = pd.to_datetime(df["fecha_ingreso"], errors="coerce")
            fechas_ingreso_invalidas = int(df["fecha_ingreso"].isna().sum())
        except Exception as e:
            fechas_ingreso_invalidas = len(df)
            df["fecha_ingreso"] = None
    else:
        fechas_ingreso_invalidas = 0
    
    if "fecha_nacimiento" in df.columns and len(df["fecha_nacimiento"]) > 0:
        try:
            df["fecha_nacimiento"] = pd.to_datetime(df["fecha_nacimiento"], errors="coerce")
            fechas_nacimiento_invalidas = int(df["fecha_nacimiento"].isna().sum())
        except Exception as e:
            fechas_nacimiento_invalidas = len(df)
            df["fecha_nacimiento"] = None
    else:
        fechas_nacimiento_invalidas = 0
    
    # ========== CALCULAR EDAD Y ANTIGÜEDAD ==========
    if "fecha_nacimiento" in df.columns and len(df["fecha_nacimiento"]) > 0:
        try:
            df["edad"] = df["fecha_nacimiento"].apply(calcular_edad)
        except Exception as e:
            df["edad"] = None
    
    if "fecha_ingreso" in df.columns and len(df["fecha_ingreso"]) > 0:
        try:
            df["antiguedad_anios"] = df["fecha_ingreso"].apply(calcular_antiguedad)
        except Exception as e:
            df["antiguedad_anios"] = None
    
    # ========== ALMACENAR ==========
    dataframes_crudos.append(df)
    
    # ========== MÉTRICAS DE CALIDAD ==========
    calidad = {
        "total_filas_recibidas": int(total_filas),
        "rut_vacios": int(rut_vacios),
        "rut_invalidos_formato": int(rut_invalidos),
        "filas_procesadas": int(len(df)),
        "remuneraciones_invalidas": int(remuneraciones_invalidas),
        "fechas_ingreso_invalidas": int(fechas_ingreso_invalidas),
        "fechas_nacimiento_invalidas": int(fechas_nacimiento_invalidas)
    }
    
    return {
        "archivo_procesado": file.filename,
        "archivos_cargados_acumulados": int(len(dataframes_crudos)),
        "calidad_datos": calidad,
        "mensaje": f"✅ Archivo almacenado. Total archivos en espera de fusión: {len(dataframes_crudos)}"
    }

@app.get("/download")
def download_file(format: str = "xlsx"):
    """
    Descarga el archivo fusionado en el formato solicitado.
    format: xlsx, csv, txt (por defecto xlsx)
    """
    if not dataframes_crudos:
        raise HTTPException(status_code=404, detail="No hay archivos cargados")
    
    df_fusionado = fusionar_todo()
    df_fusionado = rellenar_vacios(df_fusionado)
    
    if df_fusionado is None or len(df_fusionado) == 0:
        raise HTTPException(status_code=404, detail="Error al fusionar los datos")
    
    format = format.lower()
    
    if format == "xlsx":
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            df_fusionado.to_excel(writer, sheet_name='Nomina_Fusionada', index=False)
            metadata = pd.DataFrame({
                'Metrica': ['Fecha_exportacion', 'Total_ruts_unicos', 'Total_columnas', 'Archivos_fusionados'],
                'Valor': [datetime.now().strftime('%Y-%m-%d %H:%M:%S'), len(df_fusionado), len(df_fusionado.columns), len(dataframes_crudos)]
            })
            metadata.to_excel(writer, sheet_name='Metadata', index=False)
        
        buffer.seek(0)
        return StreamingResponse(
            buffer,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": "attachment; filename=nomina_fusionada.xlsx"}
        )
    
    elif format == "csv":
        buffer = io.StringIO()
        df_fusionado.to_csv(buffer, index=False, encoding='utf-8-sig')
        buffer.seek(0)
        return StreamingResponse(
            io.BytesIO(buffer.getvalue().encode('utf-8-sig')),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=nomina_fusionada.csv"}
        )
    
    elif format == "txt":
        buffer = io.StringIO()
        df_fusionado.to_csv(buffer, index=False, sep='\t', encoding='utf-8-sig')
        buffer.seek(0)
        return StreamingResponse(
            io.BytesIO(buffer.getvalue().encode('utf-8-sig')),
            media_type="text/plain",
            headers={"Content-Disposition": "attachment; filename=nomina_fusionada.txt"}
        )
    
    else:
        raise HTTPException(status_code=400, detail=f"Formato no soportado: {format}. Use: xlsx, csv, txt")

@app.get("/stats")
def get_stats():
    if not dataframes_crudos:
        return {"mensaje": "No hay archivos cargados", "archivos": 0}
    df_fusionado = fusionar_todo()
    return {
        "total_archivos": len(dataframes_crudos),
        "total_ruts_unicos": len(df_fusionado) if df_fusionado is not None else 0,
        "total_columnas": len(df_fusionado.columns) if df_fusionado is not None else 0,
        "columnas": df_fusionado.columns.tolist() if df_fusionado is not None else []
    }

# =============================================================================
# INTERFAZ WEB (v1.5 con Dropdown en botón Descargar)
# =============================================================================

@app.get("/web", response_class=HTMLResponse)
async def web_interface(request: Request):
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>iLoveDB</title>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <script src="https://cdn.tailwindcss.com"></script>
        <link rel="icon" type="image/svg+xml" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24' fill='%236b7280'%3E%3Cpath d='M12 21.35l-1.45-1.32C5.4 15.36 2 12.28 2 8.5 2 5.42 4.42 3 7.5 3c1.74 0 3.41.81 4.5 2.09C13.09 3.81 14.76 3 16.5 3 19.58 3 22 5.42 22 8.5c0 3.78-3.4 6.86-8.55 11.54L12 21.35z'/%3E%3C/svg%3E">
        <style>
            * { transition: background-color 0.4s ease-in-out, border-color 0.4s ease-in-out, color 0.4s ease-in-out, box-shadow 0.4s ease-in-out; }
            .dark body { background-color: #111827; }
            .dark .bg-white { background-color: #1f2937; }
            .dark .text-gray-500, .dark .text-gray-600 { color: #9ca3af; }
            .dark .text-gray-700, .dark .text-gray-800 { color: #e5e7eb; }
            .dark .border-gray-200, .dark .border-gray-300 { border-color: #374151; }
            .dark .bg-gray-100, .dark .bg-gray-50 { background-color: #1f2937; }
            .dark .shadow, .dark .shadow-sm, .dark .shadow-md { box-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.5); }
            .dark input[type="file"] { color: #9ca3af; }
            .dark .border-t { border-color: #374151; }
        </style>
    </head>
    <body class="bg-gray-100 min-h-screen">
        <div class="container mx-auto px-4 py-8 max-w-4xl">
            <div class="text-center mb-8">
                <div class="flex justify-end mb-4">
                    <label class="relative inline-flex items-center cursor-pointer">
                        <input type="checkbox" id="darkModeToggle" class="sr-only peer">
                        <div class="w-11 h-6 bg-gray-200 peer-focus:outline-none peer-focus:ring-4 peer-focus:ring-blue-300 dark:peer-focus:ring-blue-800 rounded-full peer dark:bg-gray-700 peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all dark:border-gray-600 peer-checked:bg-blue-600"></div>
                        <span class="ml-3 text-sm font-medium text-gray-600 dark:text-gray-300"><span id="darkModeIcon">🌞</span></span>
                    </label>
                </div>
                <h1 class="text-4xl font-bold text-gray-600 dark:text-gray-300 mb-2">Herramienta online para amantes del people analytics</h1>
                <p class="text-gray-600 dark:text-gray-400">Normaliza, limpia y fusiona todas tus bases de datos de Recursos Humanos de manera fácil y sin complicaciones. Convierte información dispersa en una HR DB ordenada y confiable, optimiza tus registros y mantén tu gestión de personas siempre al día.</p>
            </div>
            <div class="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
                <div class="bg-white rounded-xl shadow p-6 text-center">
                    <p class="text-3xl font-bold text-gray-500 dark:text-gray-400" id="archivos">0</p>
                    <p class="text-gray-500 dark:text-gray-400">Archivos cargados</p>
                </div>
                <div class="bg-white rounded-xl shadow p-6 text-center">
                    <p class="text-3xl font-bold text-gray-500 dark:text-gray-400" id="ruts">0</p>
                    <p class="text-gray-500 dark:text-gray-400">RUTs únicos</p>
                </div>
                <div class="bg-white rounded-xl shadow p-6 text-center">
                    <p class="text-3xl font-bold text-gray-500 dark:text-gray-400" id="columnas">0</p>
                    <p class="text-gray-500 dark:text-gray-400">Columnas</p>
                </div>
            </div>
            <div class="bg-white rounded-xl shadow p-6 mb-6">
                <h2 class="text-xl font-semibold mb-4 text-gray-700 dark:text-gray-300">📁 Subir nómina</h2>
                <div class="border-2 border-dashed border-gray-300 rounded-lg p-8 text-center mb-6">
                    <input type="file" id="fileInput" accept=".xlsx,.xls,.csv,.txt" multiple class="mx-auto mb-3 dark:text-gray-400">
                    <div id="fileList" class="text-sm text-gray-600 dark:text-gray-400 mt-2 text-left"></div>
                </div>
                
                <!-- BOTONES PRINCIPALES CON DROPDOWN EN DESCARGAR -->
                <div class="grid grid-cols-3 gap-4 mb-4">
                    <!-- Botón Subir -->
                    <button id="uploadBtn" class="bg-sky-500 hover:bg-sky-700 text-white py-3 rounded-lg transition font-semibold flex items-center justify-center gap-2">
                        <span class="text-2xl">↑</span> Subir
                    </button>
                    
                    <!-- Botón Descargar con Dropdown -->
                    <div class="relative">
                        <button id="downloadBtn" class="bg-sky-500 hover:bg-sky-700 text-white py-3 rounded-lg transition font-semibold flex items-center justify-center gap-2 w-full">
                            <span class="text-2xl">↓</span> Descargar
                            <span class="text-sm ml-1">▼</span>
                        </button>
                        <div id="downloadMenu" class="absolute bottom-full mb-2 left-0 w-full bg-white dark:bg-gray-800 rounded-lg shadow-lg overflow-hidden hidden z-10">
                            <button class="format-option w-full text-left px-4 py-2 hover:bg-gray-100 dark:hover:bg-gray-700 text-gray-700 dark:text-gray-200 flex items-center gap-2" data-format="xlsx">
                                📊 Excel (.xlsx)
                            </button>
                            <button class="format-option w-full text-left px-4 py-2 hover:bg-gray-100 dark:hover:bg-gray-700 text-gray-700 dark:text-gray-200 flex items-center gap-2" data-format="csv">
                                📄 CSV (.csv)
                            </button>
                            <button class="format-option w-full text-left px-4 py-2 hover:bg-gray-100 dark:hover:bg-gray-700 text-gray-700 dark:text-gray-200 flex items-center gap-2" data-format="txt">
                                📝 TXT (.txt)
                            </button>
                        </div>
                    </div>
                    
                    <!-- Botón Reiniciar -->
                    <button id="resetBtn" class="bg-rose-600 hover:bg-rose-800 text-white py-3 rounded-lg transition font-semibold flex items-center justify-center gap-2">
                        <span class="text-2xl">⟳</span> Reiniciar
                    </button>
                </div>
                
                <div id="mensaje" class="mt-4 space-y-2"></div>
            </div>
            <div class="text-center mt-8 pt-6 border-t border-gray-200 dark:border-gray-700">
                <p class="text-gray-600 dark:text-gray-400 font-medium">iLoveDB - version 1.5 2026 - I🩶DB</p>
            </div>
        </div>
        <script>
            const API_URL = '';
            const darkMode = localStorage.getItem('darkMode');
            const toggleSwitch = document.getElementById('darkModeToggle');
            const iconSpan = document.getElementById('darkModeIcon');
            if (darkMode === 'enabled') {
                document.documentElement.classList.add('dark');
                if (toggleSwitch) toggleSwitch.checked = true;
                if (iconSpan) iconSpan.innerHTML = '🌙';
            } else {
                if (iconSpan) iconSpan.innerHTML = '🌞';
            }
            function toggleDarkMode() {
                if (document.documentElement.classList.contains('dark')) {
                    document.documentElement.classList.remove('dark');
                    localStorage.setItem('darkMode', 'disabled');
                    if (iconSpan) iconSpan.innerHTML = '🌞';
                } else {
                    document.documentElement.classList.add('dark');
                    localStorage.setItem('darkMode', 'enabled');
                    if (iconSpan) iconSpan.innerHTML = '🌙';
                }
            }
            if (toggleSwitch) toggleSwitch.addEventListener('change', toggleDarkMode);
            
            const fileInput = document.getElementById('fileInput');
            const fileListDiv = document.getElementById('fileList');
            const mensajeDiv = document.getElementById('mensaje');
            
            // Dropdown de descarga
            const downloadBtn = document.getElementById('downloadBtn');
            const downloadMenu = document.getElementById('downloadMenu');
            
            downloadBtn.addEventListener('click', function(e) {
                e.stopPropagation();
                downloadMenu.classList.toggle('hidden');
            });
            
            document.addEventListener('click', function() {
                downloadMenu.classList.add('hidden');
            });
            
            async function descargarExcel(format) {
                try {
                    const res = await fetch(API_URL + `/download?format=${format}`);
                    if (res.status === 404) {
                        mostrarMensaje('No hay datos para descargar', 'error');
                        return;
                    }
                    if (res.status === 400) {
                        const error = await res.json();
                        mostrarMensaje(error.detail, 'error');
                        return;
                    }
                    const blob = await res.blob();
                    const url = window.URL.createObjectURL(blob);
                    const a = document.createElement('a');
                    a.href = url;
                    a.download = `nomina_fusionada.${format}`;
                    document.body.appendChild(a);
                    a.click();
                    document.body.removeChild(a);
                    window.URL.revokeObjectURL(url);
                    mostrarMensaje(`Descarga iniciada en formato ${format.toUpperCase()}`, 'exito');
                } catch(e) {
                    mostrarMensaje('Error al descargar', 'error');
                }
            }
            
            document.querySelectorAll('.format-option').forEach(option => {
                option.addEventListener('click', function(e) {
                    e.stopPropagation();
                    const format = this.getAttribute('data-format');
                    downloadMenu.classList.add('hidden');
                    descargarExcel(format);
                });
            });
            
            fileInput.addEventListener('change', function(e) {
                const files = Array.from(e.target.files);
                if (files.length === 0) { fileListDiv.innerHTML = ''; return; }
                let html = '<ul class="list-disc pl-5">';
                files.forEach(file => { html += `<li>📄 ${file.name}</li>`; });
                html += '</ul>';
                fileListDiv.innerHTML = html;
            });
            
            async function actualizarStats() {
                try {
                    const res = await fetch(API_URL + '/stats');
                    const data = await res.json();
                    document.getElementById('archivos').innerText = data.total_archivos || 0;
                    document.getElementById('ruts').innerText = data.total_ruts_unicos || 0;
                    document.getElementById('columnas').innerText = data.total_columnas || 0;
                } catch(e) { console.error(e); }
            }
            
            function mostrarMensaje(texto, tipo) {
                if (tipo === 'error') {
                    const errorDiv = document.createElement('div');
                    errorDiv.className = 'p-3 rounded-lg bg-red-100 text-red-700';
                    errorDiv.innerHTML = '❌ ' + texto;
                    mensajeDiv.appendChild(errorDiv);
                } else {
                    const tempDiv = document.createElement('div');
                    tempDiv.className = 'p-3 rounded-lg ' + (tipo === 'exito' ? 'bg-green-100 text-green-700' : 'bg-blue-100 text-blue-700');
                    tempDiv.innerHTML = (tipo === 'exito' ? '✅ ' : 'ℹ️ ') + texto;
                    mensajeDiv.appendChild(tempDiv);
                    setTimeout(() => { tempDiv.remove(); }, 3000);
                }
            }
            
            function limpiarMensajesError() { mensajeDiv.querySelectorAll('.bg-red-100').forEach(error => error.remove()); }
            
            async function subirArchivo() {
                const files = fileInput.files;
                if (files.length === 0) { mostrarMensaje('Selecciona uno o más archivos Excel, CSV o TXT', 'error'); return; }
                mostrarMensaje(`Subiendo ${files.length} archivo(s)...`, 'info');
                for (let i = 0; i < files.length; i++) {
                    const formData = new FormData();
                    formData.append('file', files[i]);
                    try {
                        const res = await fetch(API_URL + '/upload', { method: 'POST', body: formData });
                        const data = await res.json();
                        if (data.error) mostrarMensaje(`${files[i].name}: ${data.error}`, 'error');
                        else mostrarMensaje(`${files[i].name} procesado correctamente`, 'exito');
                    } catch(e) { mostrarMensaje(`${files[i].name}: Error de conexión`, 'error'); }
                }
                mostrarMensaje(`Proceso completado.`, 'info');
                actualizarStats();
                fileInput.value = '';
                fileListDiv.innerHTML = '';
            }
            
            async function resetDatos() {
                if (!confirm('¿Estás seguro? Se eliminarán todos los datos cargados.')) return;
                try {
                    const res = await fetch(API_URL + '/reset', { method: 'POST' });
                    const data = await res.json();
                    limpiarMensajesError();
                    mostrarMensaje(data.mensaje, 'exito');
                    actualizarStats();
                } catch(e) { mostrarMensaje('Error al reiniciar', 'error'); }
            }
            
            document.getElementById('uploadBtn').onclick = subirArchivo;
            document.getElementById('resetBtn').onclick = resetDatos;
            
            actualizarStats();
            setInterval(actualizarStats, 5000);
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

# =============================================================================
# FIN DEL ARCHIVO
# =============================================================================