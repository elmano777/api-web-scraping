import json
import boto3
import uuid
from datetime import datetime
import time

# Para usar en Lambda, necesitarás selenium y chromedriver
try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.chrome.options import Options
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False

def lambda_handler(event, context):
    if not SELENIUM_AVAILABLE:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Selenium no está disponible. Instala selenium y chromedriver.',
                'solucion': 'pip install selenium'
            })
        }
    
    url = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"
    
    # Configurar Chrome para Lambda (headless)
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920x1080")
    chrome_options.add_argument("--single-process")
    chrome_options.add_argument("--disable-dev-tools")
    chrome_options.add_argument("--no-zygote")
    
    driver = None
    try:
        # Inicializar el driver
        driver = webdriver.Chrome(options=chrome_options)
        driver.set_page_load_timeout(30)
        
        # Cargar la página
        driver.get(url)
        
        # Esperar a que se cargue el contenido dinámico
        wait = WebDriverWait(driver, 20)
        
        # Buscar diferentes selectores posibles para la tabla de sismos
        sismos_data = []
        
        try:
            # Esperar a que aparezcan elementos con datos de sismos
            # Probar diferentes selectores
            selectors_to_try = [
                "//table",
                "//div[contains(@class, 'table')]",
                "//div[contains(@class, 'sismo')]",
                "//tr[contains(., 'magnitud') or contains(., 'Magnitud')]",
                "//div[contains(text(), 'Magnitud') or contains(text(), 'magnitud')]",
                "//*[contains(text(), 'M ') or contains(text(), 'ML')]"
            ]
            
            element_found = False
            for selector in selectors_to_try:
                try:
                    elements = wait.until(EC.presence_of_all_elements_located((By.XPATH, selector)))
                    if elements:
                        print(f"Encontrados {len(elements)} elementos con selector: {selector}")
                        element_found = True
                        break
                except:
                    continue
            
            if not element_found:
                # Si no encuentra elementos específicos, esperar un tiempo y capturar todo
                time.sleep(10)
            
            # Obtener el HTML completo después de que JavaScript haya cargado
            page_source = driver.page_source
            
            # Si hay una tabla visible, extraer datos
            tables = driver.find_elements(By.TAG_NAME, "table")
            
            if tables:
                table = tables[0]  # Tomar la primera tabla
                rows = table.find_elements(By.TAG_NAME, "tr")
                
                # Extraer encabezados
                if rows:
                    header_cells = rows[0].find_elements(By.TAG_NAME, "th")
                    if not header_cells:
                        header_cells = rows[0].find_elements(By.TAG_NAME, "td")
                    
                    headers = [cell.text.strip() for cell in header_cells]
                    if not headers:
                        headers = ['Fecha', 'Hora', 'Latitud', 'Longitud', 'Profundidad', 'Magnitud', 'Ubicación']
                
                # Extraer datos de filas
                for i, row in enumerate(rows[1:11]):  # Primeros 10 sismos
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if cells and len(cells) >= 3:  # Asegurar que tenga datos
                        sismo = {
                            'id': str(uuid.uuid4()),
                            'numero': i + 1,
                            'fecha_scraping': datetime.now().isoformat()
                        }
                        
                        for j, cell in enumerate(cells):
                            header_name = headers[j] if j < len(headers) else f'campo_{j}'
                            sismo[header_name] = cell.text.strip()
                        
                        sismos_data.append(sismo)
            
            # Si no hay tabla, buscar patrones alternativos
            if not sismos_data:
                # Buscar divs o elementos que contengan información de sismos
                sismo_elements = driver.find_elements(By.XPATH, "//*[contains(text(), 'M ') or contains(text(), 'ML') or contains(text(), 'magnitud')]")
                
                for i, element in enumerate(sismo_elements[:10]):
                    sismos_data.append({
                        'id': str(uuid.uuid4()),
                        'numero': i + 1,
                        'texto_completo': element.text.strip(),
                        'fecha_scraping': datetime.now().isoformat(),
                        'tag_name': element.tag_name,
                        'html_content': element.get_attribute('innerHTML')[:200]
                    })
            
            # Si aún no hay datos, capturar información general
            if not sismos_data:
                body_text = driver.find_element(By.TAG_NAME, "body").text
                sismos_data.append({
                    'id': str(uuid.uuid4()),
                    'numero': 1,
                    'estado': 'Sin datos específicos encontrados',
                    'contenido_pagina': body_text[:500],
                    'fecha_scraping': datetime.now().isoformat()
                })
        
        except Exception as e:
            sismos_data.append({
                'id': str(uuid.uuid4()),
                'error': f'Error al extraer datos: {str(e)}',
                'fecha_scraping': datetime.now().isoformat()
            })
        
        # Guardar en DynamoDB
        if sismos_data:
            dynamodb = boto3.resource('dynamodb')
            tabla = dynamodb.Table('TablaSismosIGP')
            
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
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'mensaje': f'Procesados {len(sismos_data)} registros',
                'sismos': sismos_data,
                'total': len(sismos_data)
            }, ensure_ascii=False)
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': f'Error general: {str(e)}',
                'tipo': type(e).__name__
            })
        }
    
    finally:
        if driver:
            driver.quit()
