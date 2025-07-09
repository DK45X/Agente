import boto3
import json
import os
from dotenv import load_dotenv

load_dotenv()

bedrock_runtime = boto3.client(service_name='bedrock-runtime')

#El resto del código permanece exactamente igual.
def build_advanced_prompt(malformed_json: str) -> str:
    """Construye el prompt avanzado con las reglas de Corficolombiana."""
    # (El prompt detallado de la respuesta anterior va aquí)
    return f"""Human: Actúa como un Ingeniero de Datos experto y auditor de calidad para Corficolombiana, especializado en los lineamientos de desarrollo de ETL con DataStage. Tu tarea tiene dos partes:

**Parte 1: Corrección de Sintaxis**
Primero, corrige cualquier error de sintaxis en el siguiente archivo de configuración JSON para que sea perfectamente válido.

**Parte 2: Auditoría de Lineamientos**
Después de corregir la sintaxis, audita el contenido del JSON para verificar que cumple con los siguientes lineamientos de desarrollo de Corficolombiana:
1.  **Nomenclatura de Archivos:** Si encuentras claves que representen nombres de archivos, verifica que sigan el formato `<Fuente>_<Entidad>_<Proceso>_<Fecha>.<Extension>`.
2.  **Nomenclatura de Jobs:** Si hay nombres de jobs, deben seguir el formato `[CodProyecto]_[CodProceso]_[CodArea]_JOB_[Nombre]`.
3.  **Calidad de Datos:** Revisa si hay campos obligatorios que estén vacíos (Completitud) o si existen registros duplicados obvios dentro de un array (Unicidad).
4.  **Parámetros "Hard-Coded":** Identifica y advierte sobre cualquier valor "quemado" como rutas de archivos, nombres de servidor o credenciales. Recuérdale al usuario que debe usar los `ParameterSet` definidos (ej. `$PATH_ROOT_ENTRADA`).
5.  **Limpieza:** El flujo de trabajo descrito en el JSON debe incluir un paso final para la limpieza de archivos temporales (Datasets).

**Formato de Respuesta Obligatorio:**
Tu respuesta DEBE ser un único objeto JSON válido, sin ningún texto adicional fuera de él. El objeto JSON debe contener dos claves:
- `corrected_json`: Un string que contiene el código JSON sintácticamente corregido.
- `audit_report`: Un string en formato Markdown que resuma los errores de sintaxis corregidos y los hallazgos de la auditoría de lineamientos. Si no hay hallazgos, indícalo.

Aquí está el JSON a procesar:
<json_input>
{malformed_json}
</json_input>

Assistant:"""

def correct_and_audit_json(json_string: str) -> dict:
    """
    Invoca a Bedrock para corregir y auditar un JSON según los lineamientos de Corficolombiana.
    """
    model_id = "anthropic.claude-3-sonnet-20240229-v1:0"
    prompt = build_advanced_prompt(json_string)
    request_body = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 4096,
        "temperature": 0.0,
        "messages": [{"role": "user", "content": [{"type": "text", "text": prompt}]}],
    }
    
    try:
        response = bedrock_runtime.invoke_model(
            body=json.dumps(request_body), modelId=model_id
        )
        response_body = json.loads(response.get("body").read())
        model_response_content = response_body["content"][0]["text"]
        agent_output = json.loads(model_response_content)
        json.loads(agent_output['corrected_json'])
        return agent_output
    except Exception as e:
        print(f"Error al invocar Bedrock o procesar la respuesta: {e}")
        raise

# El ejemplo de uso no cambia.
if __name__ == "__main__":
    broken_and_non_compliant_json = """
    {
      "job_name": "MCI_TRF_FIN_JOB_CONSOLIDADO",
      "source_file": "reporte_clientes.csv",
      "source_path": "/var/temp/data/input/",
      "destination_table": "FAC_CONSOLIDADO_CLIENTES"
      "steps": [
        {"step": 1, "action": "read_source"},
        {"step": 2, "action": "transform_data"},
        {"step": 3, "action": "load_to_db"}
      ],
      "owner": ""
    }
    """
    print("--- JSON a Procesar (con errores y sin seguir lineamientos) ---")
    print(broken_and_non_compliant_json)
    
    try:
        result = correct_and_audit_json(broken_and_non_compliant_json)
        print("\n--- JSON Corregido por el Agente ---")
        corrected_json_obj = json.loads(result['corrected_json'])
        print(json.dumps(corrected_json_obj, indent=2))
        print("\n--- Informe de Auditoría del Agente ---")
        print(result['audit_report'])
    except Exception as e:
        print(f"\nNo se pudo procesar el JSON. Error: {e}")