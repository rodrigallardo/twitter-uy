# twitter-uy

Repositorio de código del proyecto "Análisis de la conversación de Twitter en Uruguay utilizando Neo4j"

## Archivos

* `download_tweets.ipynb`: Notebook que utiliza la librería `twarc2` para descargar datos de la API de Twitter.
* `load_tweets.py`: Script que utiliza `py2neo` para cargar los datos descargados en una base Neo4j.
* `consultas.ipynb`: Notebook con consultas para obtener estadisticas generales de la base y algoritmos de detección de comunidades.
* `consultas_ds.ipynb`: Notebook con algoritmos de centralidad.
* Los dumps de tweets pueden encontrarse en el siguiente directorio [compartido de Google Drive](https://drive.google.com/drive/folders/10hSboKxrx9VImw2iCJKIWMxriCbu0zI3?usp=sharing). Durante el trabajo se utilizó un subconjunto de los tweets de mayo, disponible en el archivo `may2022_uy_test.json`.

## Despliegue

Para crear su propia base con Tweets y ejecutar las consultas, debe realizar los siguientes pasos.

1. Realizar una instalación local de Neo4j o solicitar acceso en algun proveedor en la nube.
2. Instalar la librería Graph Data Science.
3. Instalar `py2neo`. `pip install py2neo`.
4. Ejecutar el script `load_tweets.py`, modificando la constante `DUMP_FILE_PATH` apuntando hacia el dump de tweets que desea cargar y las credenciales de acceso. Se recomienda probar con un dump pequeño (por ejemplo, `1hour_uy_test.json`).
5. Modificar los notebooks agregando sus credenciales de acceso y ejecutar las consultas que desee.