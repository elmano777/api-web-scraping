import json
import boto3
import uuid
from datetime import datetime
import time
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException

def lambda_handler(event, context):
    """
    Función Lambda para scraping de datos sísmicos del IGP
    usando Selenium para manejar contenido dinámico (Angular)
    """
    
    url = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"
    sismos_data = []
    
    # Configurar opciones de Chrome para Lambda
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--disable-dev-tools')
    chrome_options.add_argument('--no-zygote')
    chrome_options.add_argument('--single-process')
    chrome_options.add_argument('--window-size=1920x1080')
    chrome_options.add_argument('--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
    
    # Para AWS Lambda, necesitas usar un binary de Chrome personalizado
    # chrome_options.binary_location = '/opt/chrome/chrome'
    # chrome_options.add_argument('--single-process')
    
    try:
        # Inicializar el driver
        driver = webdriver.Chrome(options=chrome_options)
        driver.set_page_load_timeout(30)
        
        # Navegar a la página
        driver.get(url)
        
        # Esperar a que la página cargue completamente
        # Buscar indicadores de que Angular ha terminado de cargar
        wait = WebDriverWait(driver, 20)
        
        # Intentar diferentes estrategias para esperar a que se carguen los datos
        loading_strategies = [
            # Esperar por tabla de sismos
            (By.CSS_SELECTOR, 'table tbody tr'),
            (By.CSS_SELECTOR, '.sismo-item'),
            (By.CSS_SELECTOR, '[ng-repeat]'),
            (By.CSS_SELECTOR, '.table-responsive table'),
            (By.XPATH, "//table//tr[position()>1]"),
            # Esperar por contenedores de datos
            (By.CSS_SELECTOR, '.container .row'),
            (By.CSS_SELECTOR, '[data-sismo]'),
            (By.CSS_SELECTOR, '.sismos-container')
        ]
        
        element_found = None
        for by, selector in loading_strategies:
            try:
                element_found = wait.until(EC.presence_of_element_located((by, selector)))
                print(f"Elementos encontrados con selector: {selector}")
                break
            except TimeoutException:
                continue
        
        # Esperar un poco más para asegurar que todo se haya cargado
        time.sleep(3)
        
        # Intentar extraer datos de diferentes maneras
        extraction_successful = False
        
        # Método 1: Buscar tablas
        try:
            tables = driver.find_elements(By.TAG_NAME, 'table')
            if tables:
                for table in tables:
                    rows = table.find_elements(By.TAG_NAME, 'tr')
                    if len(rows) > 1:  # Tiene encabezados y datos
                        # Obtener encabezados
                        header_cells = rows[0].find_elements(By.TAG_NAME, 'th')
                        if not header_cells:
                            header_cells = rows[0].find_elements(By.TAG_NAME, 'td')
                        
                        headers = [cell.text.strip() for cell in header_cells]
                        if not headers:
                            headers = ['Fecha', 'Hora', 'Latitud', 'Longitud', 'Profundidad', 'Magnitud', 'Ubicación']
                        
                        # Procesar filas de datos
                        for i, row in enumerate(rows[1:11]):  # Máximo 10 sismos
                            cells = row.find_elements(By.TAG_NAME, 'td')
                            if cells:
                                sismo = {
                                    'id': str(uuid.uuid4()),
                                    'numero': i + 1,
                                    'fecha_scraping': datetime.now().isoformat(),
                                    'fuente': 'IGP_SELENIUM',
                                    'url_origen': url
                                }
                                
                                for j, cell in enumerate(cells):
                                    header_name = headers[j] if j < len(headers) else f'campo_{j}'
                                    sismo[header_name] = cell.text.strip()
                                
                                sismos_data.append(sismo)
                        
                        if sismos_data:
                            extraction_successful = True
                            break
        except Exception as e:
            print(f"Error extrayendo tablas: {e}")
        
        # Método 2: Buscar elementos con ng-repeat o similares (Angular)
        if not extraction_successful:
            try:
                angular_selectors = [
                    '[ng-repeat*="sismo"]',
                    '[ng-repeat*="item"]',
                    '[ng-repeat*="data"]',
                    '.sismo-item',
                    '.earthquake-item',
                    '[data-ng-repeat]'
                ]
                
                for selector in angular_selectors:
                    try:
                        elements = driver.find_elements(By.CSS_SELECTOR, selector)
                        if elements:
                            for i, element in enumerate(elements[:10]):
                                sismo = {
                                    'id': str(uuid.uuid4()),
                                    'numero': i + 1,
                                    'contenido': element.text.strip(),
                                    'fecha_scraping': datetime.now().isoformat(),
                                    'fuente': 'IGP_SELENIUM_ANGULAR',
                                    'url_origen': url,
                                    'selector_usado': selector
                                }
                                sismos_data.append(sismo)
                            extraction_successful = True
                            break
                    except Exception as e:
                        continue
            except Exception as e:
                print(f"Error con selectores Angular: {e}")
        
        # Método 3: Ejecutar JavaScript para obtener datos directamente
        if not extraction_successful:
            try:
                # Intentar obtener datos del scope de Angular
                js_scripts = [
                    """
                    var scope = angular.element(document.body).scope();
                    return scope && scope.sismos ? scope.sismos : null;
                    """,
                    """
                    return window.sismos || window.data || window.seismicData || null;
                    """,
                    """
                    var tables = document.querySelectorAll('table tbody tr');
                    var data = [];
                    for(var i = 0; i < Math.min(tables.length, 10); i++) {
                        var cells = tables[i].querySelectorAll('td');
                        var row = {};
                        for(var j = 0; j < cells.length; j++) {
                            row['campo_' + j] = cells[j].textContent.trim();
                        }
                        if(Object.keys(row).length > 0) data.push(row);
                    }
                    return data;
                    """
                ]
                
                for script in js_scripts:
                    try:
                        result = driver.execute_script(script)
                        if result and isinstance(result, list):
                            for i, item in enumerate(result[:10]):
                                sismo = {
                                    'id': str(uuid.uuid4()),
                                    'numero': i + 1,
                                    'fecha_scraping': datetime.now().isoformat(),
                                    'fuente': 'IGP_SELENIUM_JS',
                                    'url_origen': url
                                }
                                if isinstance(item, dict):
                                    sismo.update(item)
                                else:
                                    sismo['datos'] = str(item)
                                sismos_data.append(sismo)
                            extraction_successful = True
                            break
                    except Exception as e:
                        continue
            except Exception as e:
                print(f"Error ejecutando JavaScript: {e}")
        
        # Método 4: Análisis de texto completo como último recurso
        if not extraction_successful:
            try:
                # Obtener todo el texto de la página y buscar patrones
                page_text = driver.find_element(By.TAG_NAME, 'body').text
                
                # Buscar patrones de magnitud y datos sísmicos
                magnitude_pattern = r'M\s*(\d+\.\d+)'
                date_pattern = r'\d{2}/\d{2}/\d{4}'
                time_pattern = r'\d{2}:\d{2}:\d{2}'
                
                lines = page_text.split('\n')
                seismic_lines = []
                
                for line in lines:
                    if (re.search(magnitude_pattern, line, re.I) or 
                        'sismo' in line.lower() or 
                        'epicentro' in line.lower() or
                        'magnitud' in line.lower()):
                        seismic_lines.append(line.strip())
                
                for i, line in enumerate(seismic_lines[:10]):
                    if line:
                        sismos_data.append({
                            'id': str(uuid.uuid4()),
                            'numero': i + 1,
                            'texto_sismico': line,
                            'fecha_scraping': datetime.now().isoformat(),
                            'fuente': 'IGP_SELENIUM_TEXT',
                            'url_origen': url
                        })
                
                if sismos_data:
                    extraction_successful = True
            except Exception as e:
                print(f"Error con análisis de texto: {e}")
        
        # Captura de pantalla para diagnóstico (opcional)
        try:
            screenshot = driver.get_screenshot_as_base64()
            # Podrías guardar esto en S3 para diagnóstico
        except:
            pass
    
    except Exception as e:
        sismos_data.append({
            'id': str(uuid.uuid4()),
            'error': f'Error con Selenium: {str(e)}',
            'fecha_scraping': datetime.now().isoformat(),
            'tipo_error': type(e).__name__
        })
    
    finally:
        try:
            driver.quit()
        except:
            pass
    
    # Si no se obtuvieron datos, agregar información de diagnóstico
    if not sismos_data:
        sismos_data.append({
            'id': str(uuid.uuid4()),
            'estado': 'No se encontraron datos sísmicos',
            'url_analizada': url,
            'fecha_scraping': datetime.now().isoformat(),
            'tipo_error': 'SinDatos',
            'sugerencia': 'La página podría estar usando un framework JS diferente o los selectores han cambiado'
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
    
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps({
            'mensaje': f'Scraping completado. Procesados {len(sismos_data)} registros',
            'total_registros': len(sismos_data),
            'sismos': sismos_data[:3],
            'fecha_scraping': datetime.now().isoformat(),
            'fuente': 'IGP - Instituto Geofísico del Perú'
        }, ensure_ascii=False)
    }
