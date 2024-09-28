from flask import Flask, request, jsonify
from flask_cors import CORS
import json
from openai import OpenAI
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Boolean, Text, insert, JSON
from sqlalchemy.orm import sessionmaker
from threading import Thread

# Crear una instancia de Flask
app = Flask(__name__)
CORS(app)  # Permitir CORS

# Configura tu clave de API de OpenAI
client = OpenAI(api_key='')  # Reemplaza con tu clave real

# Conexión a la base de datos tracelife
engine = create_engine('mysql+mysqlconnector://root:@localhost/tracelife')
Session = sessionmaker(bind=engine)

# Definir la tabla 'desaparecidos' utilizando SQLAlchemy
metadata = MetaData()

desaparecidos = Table('desaparecidos', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('primerNombre', String(100)),
    Column('segundoNombre', String(100)),
    Column('primerApellido', String(100)),
    Column('segundoApellido', String(100)),
    Column('sexo', String(50)),
    Column('orientacionSexual', String(50)),
    Column('esPersonaTrans', Boolean),
    Column('expresionGenero', String(100)),
    Column('nacionalidad', String(100)),
    Column('fechaNacimiento', String(100)),
    Column('tipoIdentificacion', String(50)),
    Column('numeroDocumento', String(50)),
    Column('tieneDiscapacidad', Boolean),
    Column('esCampesino', Boolean),
    Column('pertenenciaEtnica', String(100)),
    Column('esVictimaConflictoArmado', Boolean),
    Column('pais', String(100)),
    Column('departamento', String(100)),
    Column('municipioResidencia', String(100)),
    Column('vereda', String(100)),
    Column('correoElectronico', String(100)),
    Column('telefonoFijo', String(20)),
    Column('telefonoCelular', String(20)),
    Column('ubicacion', String(255)),
    Column('lugarOubicacion', String(255)),
    Column('relacionConDesaparecido', String(100)),
    Column('medioContactoUBPD', String(255)),
    Column('deseaSerContactadoAFamiliar', Boolean),
    Column('tipoSolicitud', String(100)),
    Column('confidencialidad', Boolean),
    Column('archivosAdjuntos', JSON),
    Column('nombreDesaparecido', String(255)),
    Column('fechaDesaparicion', String(100)),
    Column('circunstanciasDesaparicion', Text),
    Column('fechaDesplazamiento', String(100)),
    Column('quelepaso', Text),
    Column('vivaomuerta', String(100)),
    Column('primeraubicaciondeldesaparecido', String(255)),
    Column('ultimaubicaciondeldesaparecido', String(255)),
    Column('grupoPersonaResponsableDelDesaparecimiento', String(255)),
    Column('relatoCompleto', Text),
    Column('causaDesplazamiento', Text)
)

# Crear la tabla si no existe
metadata.create_all(engine)

# Función para interactuar con la API de OpenAI (ChatGPT)
def consulta_chatgpt(relato):
    system_message = """
    Eres un asistente que organiza relatos de desaparecidos en formato JSON. 
    Recuerda extraer los datos claves de acuerdo al siguiente formato:
    Si se menciona un grupo armado (FARC, ejército, guerrilla), asigna `"esVictimaConflictoArmado": true`.
    Si un dato no está presente, usa null.

    Aquí está el formato esperado para el JSON:

    {
      "Nombre": null,
      "segundoNombre": null,
      "primerApellido": null,
      "segundoApellido": null,
      "sexo": null,
      "orientacionSexual": null,
      "esPersonaTrans": false,
      "expresionGenero": null,
      "nacionalidad": "Colombiano",
      "fechaNacimiento": null,
      "tipoIdentificacion": null,
      "numeroDocumento": null,
      "tieneDiscapacidad": false,
      "esCampesino": true,
      "pertenenciaEtnica": null,
      "esVictimaConflictoArmado": null,
      "pais": "Colombia",
      "departamento": null,
      "municipioResidencia": "Pueblito",
      "vereda": null,
      "correoElectronico": null,
      "telefonoFijo": null,
      "telefonoCelular": null,
      "ubicacion": null,
      "lugarOubicacion": null,
      "relacionConDesaparecido": null,
      "medioContactoUBPD": null,
      "deseaSerContactadoAFamiliar": null,
      "tipoSolicitud": "Búsqueda de persona desaparecida",
      "confidencialidad": null,
      "archivosAdjuntos": [],
      "nombreDesaparecido": null,
      "fechaDesaparicion": null,
      "circunstanciasDesaparicion": null,
      "fechaDesplazamiento": null,
      "quelepaso": null,
      "vivaomuerta": null,
      "primeraubicaciondeldesaparecido": null,
      "ultimaubicaciondeldesaparecido": null,
      "grupoPersonaResponsableDelDesaparecimiento": null,
      "relatoCompleto": null,
      "causaDesplazamiento": null
    }
    """

    messages = [
        {"role": "system", "content": system_message},
        {"role": "user", "content": f"Relato:\n{relato}\n\nPor favor organiza la información en formato JSON."}
    ]

    response = client.chat.completions.create(
        model="gpt-4o-mini",  
        messages=messages,
        temperature=0
    )

    return response.choices[0].message.content

# Función para convertir cadenas a booleanos
def convertir_a_boolean(valor):
    if valor is None:
        return None
    if isinstance(valor, bool):
        return valor
    if isinstance(valor, str):
        valor = valor.strip().lower()
        if valor in ['si', 'true', '1']:
            return True
        elif valor in ['no', 'false', '0']:
            return False
    return False

@app.route('/procesar_relato', methods=['POST'])
def procesar_relato():
    session = Session()  # Crear una nueva sesión en cada solicitud
    try:
        relato = request.data.decode('utf-8')  
        if not relato:
            return jsonify({"error": "No se proporcionó un relato"}), 400

        # Enviar el relato a ChatGPT
        respuesta = consulta_chatgpt(relato)

        respuesta = respuesta.replace('```json', '').replace('```', '')

        try:
            datos = json.loads(respuesta)
        except json.JSONDecodeError as e:
            return jsonify({"error": f"Error al decodificar el JSON: {e}"}), 500

        # Convertir campos booleanos y manejar 'archivosAdjuntos'
        boolean_fields = ['esPersonaTrans', 'tieneDiscapacidad', 'esCampesino', 'esVictimaConflictoArmado', 'deseaSerContactadoAFamiliar', 'confidencialidad']
        for field in boolean_fields:
            datos[field] = convertir_a_boolean(datos.get(field))

        archivos_adjuntos = datos.get("archivosAdjuntos")
        if isinstance(archivos_adjuntos, (list, dict)):
            archivos_adjuntos = json.dumps(archivos_adjuntos)
        elif archivos_adjuntos is not None:
            archivos_adjuntos = str(archivos_adjuntos)
        else:
            archivos_adjuntos = None

        # Preparar la inserción a la base de datos
        insert_stmt = insert(desaparecidos).values(
            primerNombre=datos.get("Nombre"),
            segundoNombre=datos.get("segundoNombre"),
            primerApellido=datos.get("primerApellido"),
            segundoApellido=datos.get("segundoApellido"),
            sexo=datos.get("sexo"),
            orientacionSexual=datos.get("orientacionSexual"),
            esPersonaTrans=datos.get("esPersonaTrans"),
            expresionGenero=datos.get("expresionGenero"),
            nacionalidad=datos.get("nacionalidad"),
            fechaNacimiento=datos.get("fechaNacimiento"),
            tipoIdentificacion=datos.get("tipoIdentificacion"),
            numeroDocumento=datos.get("numeroDocumento"),
            tieneDiscapacidad=datos.get("tieneDiscapacidad"),
            esCampesino=datos.get("esCampesino"),
            pertenenciaEtnica=datos.get("pertenenciaEtnica"),
            esVictimaConflictoArmado=datos.get("esVictimaConflictoArmado"),
            pais=datos.get("pais"),
            departamento=datos.get("departamento"),
            municipioResidencia=datos.get("municipioResidencia"),
            vereda=datos.get("vereda"),
            correoElectronico=datos.get("correoElectronico"),
            telefonoFijo=datos.get("telefonoFijo"),
            telefonoCelular=datos.get("telefonoCelular"),
            ubicacion=datos.get("ubicacion"),
            lugarOubicacion=datos.get("lugarOubicacion"),
            relacionConDesaparecido=datos.get("relacionConDesaparecido"),
            medioContactoUBPD=datos.get("medioContactoUBPD"),
            deseaSerContactadoAFamiliar=datos.get("deseaSerContactadoAFamiliar"),
            tipoSolicitud=datos.get("tipoSolicitud"),
            confidencialidad=datos.get("confidencialidad"),
            archivosAdjuntos=archivos_adjuntos,
            nombreDesaparecido=datos.get("nombreDesaparecido"),
            fechaDesaparicion=datos.get("fechaDesaparicion"),
            circunstanciasDesaparicion=datos.get("circunstanciasDesaparicion"),
            fechaDesplazamiento=datos.get("fechaDesplazamiento"),
            quelepaso=datos.get("quelepaso"),
            vivaomuerta=datos.get("vivaomuerta"),
            primeraubicaciondeldesaparecido=datos.get("primeraubicaciondeldesaparecido"),
            ultimaubicaciondeldesaparecido=datos.get("ultimaubicaciondeldesaparecido"),
            grupoPersonaResponsableDelDesaparecimiento=datos.get("grupoPersonaResponsableDelDesaparecimiento"),
            relatoCompleto=datos.get("relatoCompleto"),
            causaDesplazamiento=datos.get("causaDesplazamiento")
        )

        # Ejecutar la inserción
        session.execute(insert_stmt)
        session.commit()

        # Devolver la respuesta con el JSON generado por ChatGPT
        return jsonify({"message": "Procesamiento e inserción exitosos", "datos": datos})

    except Exception as e:
        session.rollback()  # Rollback si ocurre un error
        print(f"Error durante la inserción: {e}")  # Agregar logs para ver el error
        return jsonify({"error": f"Hubo un error al procesar el relato: {e}"}), 500
    finally:
        session.close()  # Asegurarse de cerrar la sesión

@app.after_request
def apply_cors_headers(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "POST, GET, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    return response

def run_app():
    app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=False)

# Iniciar Flask en un hilo para no bloquear Jupyter
Thread(target=run_app).start()
