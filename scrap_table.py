import json
import boto3
import uuid
from datetime import datetime
import requests
import re
from bs4 import BeautifulSoup
import time

def lambda_handler(event, context):
    """
    Función Lambda para scraping de datos sísmicos del IGP
    usando ScrapingBee para manejar contenido dinámico (Angular)
    """
    
    # Configuración de ScrapingBee
    SCRAPINGBEE_API_KEY = "XI2EJYTI4PNGLQVKRJ6FL30GXOEZ4JIV7MGN8E6AK30CFUUQUEHXFWASE58CF80MX22AQOYZJTSBVAV7"
    BASE_URL = "https://app.scrapingbee.com/api/v1/"
    
    target_url = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"
    sismos_data = []
    
    try:
        # Parámetros para ScrapingBee
        params = {
            'api_key': SCRAPINGBEE_API_KEY,
            'url': target_url,
            'render_js': 'true',  # Renderizar JavaScript (Angular)
            'wait': 5000,  # Esperar 5 segundos para que Angular cargue
            'wait_for': 'table',  # Esperar hasta que aparezca una tabla
            'premium_proxy': 'true',  # Usar proxies premium
            'country_code': 'PE',  # Usar proxy de Perú si está disponible
            'device': 'desktop',
            'block_ads': 'true',
            'block_resources': 'false'  # No bloquear recursos para Angular
        }
        
        # Realizar la solicitud a ScrapingBee
        print(f"Iniciando scraping de: {target_url}")
        response = requests.get(BASE_URL, params=params, timeout=120)
        
        if response.status_code == 200:
            html_content = response.text
            
            # Usar BeautifulSoup para parsear el HTML
            soup = BeautifulSoup(html_content, 'html.parser')
            
            extraction_successful = False
            
            # Método 1: Buscar tablas específicas de sismos
            try:
                tables = soup.find_all('table')
                print(f"Encontradas {len(tables)} tablas")
                
                for table_idx, table in enumerate(tables):
                    rows = table.find_all('tr')
                    if len(rows) > 1:  # Tiene encabezados y datos
                        # Obtener encabezados
                        header_row = rows[0]
                        header_cells = header_row.find_all(['th', 'td'])
                        headers = [cell.get_text(strip=True) for cell in header_cells]
                        
                        # Si no hay encabezados claros, usar valores por defecto
                        if not headers or all(not h for h in headers):
                            headers = ['Fecha', 'Hora', 'Latitud', 'Longitud', 'Profundidad', 'Magnitud', 'Ubicación']
                        
                        print(f"Tabla {table_idx} - Encabezados: {headers}")
                        
                        # Procesar filas de datos
                        data_rows = 0
                        for i, row in enumerate(rows[1:]):  # Saltar encabezados
                            if data_rows >= 10:  # Máximo 10 sismos
                                break
                                
                            cells = row.find_all('td')
                            if cells and any(cell.get_text(strip=True) for cell in cells):
                                sismo = {
                                    'id': str(uuid.uuid4()),
                                    'numero': data_rows + 1,
                                    'fecha_scraping': datetime.now().isoformat(),
                                    'fuente': 'IGP_SCRAPINGBEE',
                                    'url_origen': target_url,
                                    'tabla_numero': table_idx
                                }
                                
                                # Mapear celdas a encabezados
                                for j, cell in enumerate(cells):
                                    cell_text = cell.get_text(strip=True)
                                    if cell_text:  # Solo agregar si hay contenido
                                        header_name = headers[j] if j < len(headers) else f'campo_{j}'
                                        sismo[header_name] = cell_text
                                
                                # Validar que el registro tenga datos útiles
                                if len([v for v in sismo.values() if isinstance(v, str) and v.strip()]) > 5:
                                    sismos_data.append(sismo)
                                    data_rows += 1
                        
                        if sismos_data:
                            extraction_successful = True
                            print(f"Extraídos {len(sismos_data)} sismos de la tabla {table_idx}")
                            break
                            
            except Exception as e:
                print(f"Error extrayendo tablas: {e}")
            
            # Método 2: Buscar elementos específicos con clases de Angular/sismos
            if not extraction_successful:
                try:
                    # Selectores comunes para datos sísmicos
                    selectors = [
                        '.sismo-item', '.earthquake-item', '.seismic-data',
                        '[ng-repeat*="sismo"]', '[ng-repeat*="earthquake"]',
                        '.sismos-container .row', '.data-row',
                        '[data-sismo]', '[data-earthquake]'
                    ]
                    
                    for selector in selectors:
                        elements = soup.select(selector)
                        if elements:
                            print(f"Encontrados {len(elements)} elementos con selector: {selector}")
                            
                            for i, element in enumerate(elements[:10]):
                                text_content = element.get_text(strip=True)
                                if text_content and len(text_content) > 10:  # Filtrar elementos vacíos
                                    sismo = {
                                        'id': str(uuid.uuid4()),
                                        'numero': i + 1,
                                        'contenido_completo': text_content,
                                        'fecha_scraping': datetime.now().isoformat(),
                                        'fuente': 'IGP_SCRAPINGBEE_SELECTOR',
                                        'url_origen': target_url,
                                        'selector_usado': selector
                                    }
                                    
                                    # Intentar extraer datos específicos del texto
                                    extract_seismic_data_from_text(text_content, sismo)
                                    sismos_data.append(sismo)
                            
                            if sismos_data:
                                extraction_successful = True
                                break
                                
                except Exception as e:
                    print(f"Error con selectores específicos: {e}")
            
            # Método 3: Análisis de texto completo para encontrar patrones sísmicos
            if not extraction_successful:
                try:
                    # Obtener todo el texto visible
                    all_text = soup.get_text()
                    lines = [line.strip() for line in all_text.split('\n') if line.strip()]
                    
                    # Patrones para identificar datos sísmicos
                    seismic_patterns = [
                        r'M\s*\d+\.\d+',  # Magnitud
                        r'\d{2}/\d{2}/\d{4}.*\d{2}:\d{2}:\d{2}',  # Fecha y hora
                        r'profundidad.*\d+.*km',  # Profundidad
                        r'epicentro',  # Epicentro
                        r'lat.*\d+\.\d+.*lon.*\d+\.\d+'  # Coordenadas
                    ]
                    
                    seismic_lines = []
                    for line in lines:
                        line_lower = line.lower()
                        if (any(re.search(pattern, line, re.I) for pattern in seismic_patterns) or
                            any(keyword in line_lower for keyword in ['sismo', 'earthquake', 'temblor', 'epicentro'])):
                            seismic_lines.append(line)
                    
                    print(f"Encontradas {len(seismic_lines)} líneas con contenido sísmico")
                    
                    for i, line in enumerate(seismic_lines[:10]):
                        if len(line) > 20:  # Filtrar líneas muy cortas
                            sismo = {
                                'id': str(uuid.uuid4()),
                                'numero': i + 1,
                                'texto_sismico': line,
                                'fecha_scraping': datetime.now().isoformat(),
                                'fuente': 'IGP_SCRAPINGBEE_TEXT',
                                'url_origen': target_url
                            }
                            
                            # Extraer datos específicos del texto
                            extract_seismic_data_from_text(line, sismo)
                            sismos_data.append(sismo)
                    
                    if sismos_data:
                        extraction_successful = True
                
                except Exception as e:
                    print(f"Error con análisis de texto: {e}")
            
            # Información adicional de ScrapingBee
            scrapingbee_info = {
                'response_headers': dict(response.headers),
                'status_code': response.status_code,
                'content_length': len(html_content)
            }
            
        else:
            # Error en la solicitud a ScrapingBee
            error_info = {
                'id': str(uuid.uuid4()),
                'error': f'Error ScrapingBee: HTTP {response.status_code}',
                'response_text': response.text[:500],  # Primeros 500 caracteres del error
                'fecha_scraping': datetime.now().isoformat(),
                'tipo_error': 'ScrapingBeeHTTPError'
            }
            sismos_data.append(error_info)
    
    except requests.exceptions.Timeout:
        sismos_data.append({
            'id': str(uuid.uuid4()),
            'error': 'Timeout en ScrapingBee - La página tardó demasiado en cargar',
            'fecha_scraping': datetime.now().isoformat(),
            'tipo_error': 'Timeout'
        })
    
    except Exception as e:
        sismos_data.append({
            'id': str(uuid.uuid4()),
            'error': f'Error general: {str(e)}',
            'fecha_scraping': datetime.now().isoformat(),
            'tipo_error': type(e).__name__
        })
    
    # Si no se obtuvieron datos, agregar información de diagnóstico
    if not sismos_data:
        sismos_data.append({
            'id': str(uuid.uuid4()),
            'estado': 'No se encontraron datos sísmicos',
            'url_analizada': target_url,
            'fecha_scraping': datetime.now().isoformat(),
            'tipo_error': 'SinDatos',
            'sugerencia': 'La página podría haber cambiado su estructura o no contener datos en el momento del scraping'
        })
    
    # Guardar en DynamoDB
    try:
        if sismos_data:
            dynamodb = boto3.resource('dynamodb')
            tabla = dynamodb.Table('TablaWebScrapping')
            
            # Limpiar registros anteriores
            try:
                scan = tabla.scan()
                with tabla.batch_writer() as batch:
                    for item in scan['Items']:
                        batch.delete_item(Key={'id': item['id']})
                print(f"Tabla limpiada: {len(scan.get('Items', []))} registros eliminados")
            except Exception as e:
                print(f"Error al limpiar tabla: {e}")
            
            # Insertar nuevos datos
            inserted_count = 0
            for sismo in sismos_data:
                try:
                    tabla.put_item(Item=sismo)
                    inserted_count += 1
                except Exception as e:
                    print(f"Error al insertar registro: {e}")
            
            print(f"Insertados {inserted_count} registros en DynamoDB")
    
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': f'Error con DynamoDB: {str(e)}',
                'datos_extraidos': len(sismos_data)
            })
        }
    
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps({
            'mensaje': f'Scraping completado con ScrapingBee. Procesados {len(sismos_data)} registros',
            'total_registros': len(sismos_data),
            'sismos': sismos_data[:3] if len(sismos_data) > 3 else sismos_data,
            'fecha_scraping': datetime.now().isoformat(),
            'fuente': 'IGP - Instituto Geofísico del Perú',
            'metodo': 'ScrapingBee API'
        }, ensure_ascii=False)
    }

def extract_seismic_data_from_text(text, sismo_dict):
    """
    Extrae datos sísmicos específicos de un texto usando expresiones regulares
    """
    try:
        # Patrones para extraer datos específicos
        patterns = {
            'magnitud': r'M\s*(\d+\.\d+)',
            'fecha': r'(\d{1,2}/\d{1,2}/\d{4})',
            'hora': r'(\d{1,2}:\d{2}:\d{2})',
            'profundidad': r'(\d+(?:\.\d+)?)\s*km',
            'latitud': r'lat[^0-9-]*(-?\d+\.\d+)',
            'longitud': r'lon[^0-9-]*(-?\d+\.\d+)'
        }
        
        for key, pattern in patterns.items():
            match = re.search(pattern, text, re.I)
            if match:
                sismo_dict[key] = match.group(1)
        
        # Buscar ubicación (texto después de coordenadas o al final)
        location_patterns = [
            r'(?:ubicación|location|lugar)[:\s]*([^0-9\n]+?)(?:\d|$)',
            r'(?:lat.*?lon.*?)([A-Za-z][^0-9\n]+?)(?:\d|$)',
            r'([A-Z][^0-9\n]{10,}?)(?:\d|$)'
        ]
        
        for pattern in location_patterns:
            match = re.search(pattern, text, re.I)
            if match:
                location = match.group(1).strip()
                if len(location) > 5:  # Filtrar ubicaciones muy cortas
                    sismo_dict['ubicacion'] = location
                    break
                    
    except Exception as e:
        print(f"Error extrayendo datos del texto: {e}")
