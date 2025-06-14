import json
import boto3
import uuid
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import time
import re

def lambda_handler(event, context):
    """
    Función Lambda para scraping de datos sísmicos del IGP
    usando requests + BeautifulSoup en lugar de Selenium
    """
    
    # URL del IGP
    url = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"
    
    # Headers para simular un navegador real
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    }
    
    sismos_data = []
    
    try:
        # Realizar petición HTTP
        session = requests.Session()
        
        # Primero intentar endpoints de API conocidos del IGP
        api_endpoints = [
            "https://ultimosismo.igp.gob.pe/ultimo-sismo/ajax/sismos/reportados",
            "https://ultimosismo.igp.gob.pe/ultimo-sismo/api/sismos",
            "https://ultimosismo.igp.gob.pe/api/sismos-reportados",
            "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados/data",
            "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados?format=json"
        ]
        
        # Intentar obtener datos de APIs
        for api_url in api_endpoints:
            try:
                api_response = session.get(api_url, headers=headers, timeout=15)
                if api_response.status_code == 200:
                    try:
                        data = api_response.json()
                        if data and isinstance(data, (list, dict)):
                            # Procesar datos de API
                            if isinstance(data, list):
                                for i, sismo in enumerate(data[:10]):
                                    sismo_record = {
                                        'id': str(uuid.uuid4()),
                                        'numero': i + 1,
                                        'fecha_scraping': datetime.now().isoformat(),
                                        'fuente': 'IGP_API',
                                        'url_origen': api_url
                                    }
                                    if isinstance(sismo, dict):
                                        sismo_record.update(sismo)
                                    else:
                                        sismo_record['datos'] = str(sismo)
                                    sismos_data.append(sismo_record)
                            elif isinstance(data, dict) and 'sismos' in data:
                                for i, sismo in enumerate(data['sismos'][:10]):
                                    sismo_record = {
                                        'id': str(uuid.uuid4()),
                                        'numero': i + 1,
                                        'fecha_scraping': datetime.now().isoformat(),
                                        'fuente': 'IGP_API',
                                        'url_origen': api_url
                                    }
                                    sismo_record.update(sismo)
                                    sismos_data.append(sismo_record)
                            
                            if sismos_data:
                                break  # Si encontramos datos, salir del loop
                    except json.JSONDecodeError:
                        # No es JSON válido, continuar
                        continue
            except requests.exceptions.RequestException:
                continue
        
        # Si no obtuvimos datos de APIs, intentar scraping HTML
        if not sismos_data:
            response = session.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            # Parsear el HTML
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Buscar scripts que puedan contener datos JSON
            scripts = soup.find_all('script')
            for script in scripts:
                if script.string:
                    script_content = script.string
                    # Buscar patrones JSON en JavaScript
                    json_patterns = [
                        r'sismos\s*[:=]\s*(\[.*?\])',
                        r'data\s*[:=]\s*(\[.*?\])',
                        r'reportados\s*[:=]\s*(\[.*?\])',
                        r'var\s+sismos\s*=\s*(\[.*?\]);',
                        r'let\s+sismos\s*=\s*(\[.*?\]);'
                    ]
                    
                    for pattern in json_patterns:
                        matches = re.search(pattern, script_content, re.DOTALL | re.IGNORECASE)
                        if matches:
                            try:
                                json_data = json.loads(matches.group(1))
                                if json_data and isinstance(json_data, list):
                                    for i, sismo in enumerate(json_data[:10]):
                                        sismo_record = {
                                            'id': str(uuid.uuid4()),
                                            'numero': i + 1,
                                            'fecha_scraping': datetime.now().isoformat(),
                                            'fuente': 'IGP_JS_DATA',
                                            'url_origen': url
                                        }
                                        if isinstance(sismo, dict):
                                            sismo_record.update(sismo)
                                        else:
                                            sismo_record['datos'] = str(sismo)
                                        sismos_data.append(sismo_record)
                                    break
                            except json.JSONDecodeError:
                                continue
                    
                    if sismos_data:
                        break
            
            # Buscar tablas de sismos si no encontramos datos en JS
            if not sismos_data:
                tables = soup.find_all('table')
        
        if tables:
            # Procesar la primera tabla encontrada
            table = tables[0]
            rows = table.find_all('tr')
            
            # Extraer encabezados
            headers_row = rows[0] if rows else None
            headers = []
            
            if headers_row:
                header_cells = headers_row.find_all(['th', 'td'])
                headers = [cell.get_text(strip=True) for cell in header_cells]
            
            # Si no hay headers, usar valores por defecto
            if not headers:
                headers = ['Fecha', 'Hora', 'Latitud', 'Longitud', 'Profundidad', 'Magnitud', 'Ubicación']
            
            # Procesar filas de datos
            data_rows = rows[1:] if len(rows) > 1 else rows
            
            for i, row in enumerate(data_rows[:10]):  # Limitar a 10 sismos
                cells = row.find_all(['td', 'th'])
                
                if cells and len(cells) >= 3:  # Verificar que tenga datos suficientes
                    sismo = {
                        'id': str(uuid.uuid4()),
                        'numero': i + 1,
                        'fecha_scraping': datetime.now().isoformat(),
                        'fuente': 'IGP',
                        'url_origen': url
                    }
                    
                    # Mapear datos a headers
                    for j, cell in enumerate(cells):
                        header_name = headers[j] if j < len(headers) else f'campo_{j}'
                        cell_text = cell.get_text(strip=True)
                        sismo[header_name] = cell_text
                    
                    sismos_data.append(sismo)
        
        # Si no se encontraron tablas, buscar otros patrones
        if not sismos_data:
            # Buscar divs o elementos que contengan información sísmica
            if not sismos_data:
                # Buscar por clases CSS comunes para datos sísmicos
                sismo_containers = soup.find_all(['div', 'section', 'article'], class_=re.compile(r'sismo|earthquake|seism|reporte', re.I))
                
                for container in sismo_containers[:5]:
                    text_content = container.get_text(strip=True)
                    if len(text_content) > 50:  # Solo contenedores con contenido sustancial
                        sismos_data.append({
                            'id': str(uuid.uuid4()),
                            'numero': len(sismos_data) + 1,
                            'contenido': text_content[:300],
                            'fecha_scraping': datetime.now().isoformat(),
                            'fuente': 'IGP_CSS_CONTAINER',
                            'url_origen': url,
                            'html_class': container.get('class', [])
                        })
                
                # Si aún no hay datos, buscar elementos con texto que contenga patrones sísmicos
                if not sismos_data:
                    sismo_elements = soup.find_all(text=re.compile(r'M\s*\d+\.\d+|magnitud|Magnitud|sismo|epicentro|profundidad.*km', re.I))
                    
                    for i, element in enumerate(sismo_elements[:10]):
                        parent = element.parent
                        if parent:
                            sismos_data.append({
                                'id': str(uuid.uuid4()),
                                'numero': i + 1,
                                'texto_encontrado': str(element).strip()[:100],
                                'contexto_padre': parent.get_text(strip=True)[:200],
                                'fecha_scraping': datetime.now().isoformat(),
                                'fuente': 'IGP_TEXTO_PATRON',
                                'url_origen': url,
                                'tag_padre': parent.name
                            })
            
            # Intentar obtener información de metadatos o headers específicos
            if not sismos_data:
                # Buscar divs que puedan contener información de carga dinámica
                loading_elements = soup.find_all(['div', 'section'], class_=re.compile(r'loading|content|main|data', re.I))
                
                for element in loading_elements[:3]:
                    if element.get('id') or element.get('class'):
                        sismos_data.append({
                            'id': str(uuid.uuid4()),
                            'numero': len(sismos_data) + 1,
                            'elemento_id': element.get('id', ''),
                            'elemento_class': element.get('class', []),
                            'contenido_parcial': element.get_text(strip=True)[:200],
                            'fecha_scraping': datetime.now().isoformat(),
                            'fuente': 'IGP_STRUCTURE_ANALYSIS',
                            'url_origen': url,
                            'nota': 'Elemento que podría contener datos dinámicos'
                        })
        
        # Si aún no hay datos, intentar buscar la API o endpoints JSON
        if not sismos_data:
            # Intentar buscar llamadas AJAX o APIs
            try:
                # Algunas páginas del IGP pueden tener endpoints JSON
                api_urls = [
                    "https://ultimosismo.igp.gob.pe/api/ultimo-sismo/sismos-reportados",
                    "https://ultimosismo.igp.gob.pe/ultimo-sismo/api/sismos",
                    "https://ultimosismo.igp.gob.pe/ultimo-sismo/data/sismos.json"
                ]
                
                for api_url in api_urls:
                    try:
                        api_response = session.get(api_url, headers=headers, timeout=10)
                        if api_response.status_code == 200:
                            data = api_response.json()
                            if isinstance(data, list) and data:
                                for i, sismo in enumerate(data[:10]):
                                    sismo_record = {
                                        'id': str(uuid.uuid4()),
                                        'numero': i + 1,
                                        'fecha_scraping': datetime.now().isoformat(),
                                        'fuente': 'IGP_API',
                                        'url_origen': api_url
                                    }
                                    sismo_record.update(sismo)
                                    sismos_data.append(sismo_record)
                                break
                    except:
                        continue
            except:
                pass
        
        # Si aún no hay datos, capturar información de diagnóstico
        if not sismos_data:
            # Analizar la estructura de la página para diagnóstico
            page_info = {
                'id': str(uuid.uuid4()),
                'numero': 1,
                'estado': 'Análisis de estructura de página',
                'html_title': soup.title.string if soup.title else 'Sin título',
                'respuesta_status': response.status_code if 'response' in locals() else 'Sin respuesta',
                'total_scripts': len(soup.find_all('script')),
                'total_divs': len(soup.find_all('div')),
                'total_tables': len(soup.find_all('table')),
                'fecha_scraping': datetime.now().isoformat(),
                'fuente': 'IGP_DIAGNOSTICO',
                'url_origen': url
            }
            
            # Buscar elementos que sugieran contenido dinámico
            dynamic_indicators = soup.find_all(['div', 'section'], attrs={'id': re.compile(r'app|vue|react|angular|data|content', re.I)})
            if dynamic_indicators:
                page_info['indicadores_dinamicos'] = [elem.get('id') for elem in dynamic_indicators[:3]]
            
            # Buscar noscript que indique dependencia de JavaScript
            noscript = soup.find('noscript')
            if noscript:
                page_info['mensaje_noscript'] = noscript.get_text(strip=True)[:100]
            
            sismos_data.append(page_info)
    
    except requests.exceptions.RequestException as e:
        sismos_data.append({
            'id': str(uuid.uuid4()),
            'error': f'Error de conexión: {str(e)}',
            'fecha_scraping': datetime.now().isoformat(),
            'tipo_error': 'RequestException'
        })
    
    except Exception as e:
        sismos_data.append({
            'id': str(uuid.uuid4()),
            'error': f'Error general: {str(e)}',
            'fecha_scraping': datetime.now().isoformat(),
            'tipo_error': type(e).__name__
        })
    
    # Guardar en DynamoDB
    try:
        if sismos_data:
            dynamodb = boto3.resource('dynamodb')
            
            # Usar el nombre correcto de la tabla según tu serverless.yml
            tabla = dynamodb.Table('TablaWebScrapping')  # Cambié de TablaSismosIGP
            
            # Limpiar registros anteriores
            try:
                scan = tabla.scan()
                with tabla.batch_writer() as batch:
                    for item in scan['Items']:
                        batch.delete_item(Key={'id': item['id']})
            except Exception as e:
                print(f"Error al limpiar tabla: {e}")
            
            # Insertar nuevos datos
            for sismo in sismos_data:
                try:
                    tabla.put_item(Item=sismo)
                except Exception as e:
                    print(f"Error al insertar: {e}")
    
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': f'Error con DynamoDB: {str(e)}',
                'datos_extraidos': len(sismos_data)
            })
        }
    
    # Respuesta exitosa
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps({
            'mensaje': f'Scraping completado. Procesados {len(sismos_data)} registros',
            'total_registros': len(sismos_data),
            'sismos': sismos_data[:3],  # Solo los primeros 3 para la respuesta
            'fecha_scraping': datetime.now().isoformat(),
            'fuente': 'IGP - Instituto Geofísico del Perú'
        }, ensure_ascii=False)
    }
