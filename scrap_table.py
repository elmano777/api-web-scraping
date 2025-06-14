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
        response = session.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        # Parsear el HTML
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Buscar tablas de sismos
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
            sismo_elements = soup.find_all(text=re.compile(r'M\s*\d+\.\d+|magnitud|Magnitud'))
            
            for i, element in enumerate(sismo_elements[:5]):
                parent = element.parent
                if parent:
                    sismos_data.append({
                        'id': str(uuid.uuid4()),
                        'numero': i + 1,
                        'texto_encontrado': str(element).strip(),
                        'contexto': parent.get_text(strip=True)[:200],
                        'fecha_scraping': datetime.now().isoformat(),
                        'fuente': 'IGP_texto',
                        'url_origen': url
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
        
        # Si no hay datos, registrar el intento
        if not sismos_data:
            sismos_data.append({
                'id': str(uuid.uuid4()),
                'numero': 1,
                'estado': 'No se encontraron datos sísmicos',
                'html_title': soup.title.string if soup.title else 'Sin título',
                'respuesta_status': response.status_code,
                'fecha_scraping': datetime.now().isoformat(),
                'fuente': 'IGP_fallback',
                'url_origen': url
            })
    
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
