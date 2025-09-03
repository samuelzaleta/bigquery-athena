import pandas as pd
import numpy as np
from pandas import json_normalize


# --- Limpieza y Preparación de Datos ---
def clean_transform_data(df: pd.DataFrame):
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True).dt.tz_localize(None)

    resource_df = json_normalize(df["resource"])
    json_df = json_normalize(df["jsonPayload"])
    json_df.columns = [col.replace("jsonPayload.", "") for col in json_df.columns]

    """
    #Verificar campos que eliminan del excel los 
    jsonPayload.gemini_final_response.
    jsonPayload.intent_information.
    jsonPayload.session_attributes.
    resource.labels.
    """
    base_cols = [
        'timestamp', 'intent_name', 'knowledge_domain', 'origin_channel',
        'transactional_or_non_transactional', 'configuration_name',
        'conversation_log', 'inputTranscript', 'final_response', 'botName',
        'inputMode', 'sessionid', 'slot_type', 'curp', 'correo', 'correoElectronico', 'correo_WA', 'email',
        'telefono', 'numeroCelular', 'phoneNumber', 'tel1', 'telefono1_WA', 'telefonos', 'tels'
    ]

    resource_map = {
        "type": "resource_type",
        "labels.configuration_name": "configuration_name",
        "labels.project_id": "project_id",
        "labels.location": "location",
        "labels.service_name": "service_name",
        "labels.revision_name": "revision_name",
    }
    resource_df = resource_df.rename(columns=resource_map)
    column_map = {
        # intent_information
        "intent_information.intent_name": "intent_name",
        "intent_information.knowledge_domain": "knowledge_domain",
        "intent_information.origin_channel": "origin_channel",
        "intent_information.transactional_or_non_transactional": "transactional_or_non_transactional",

        # gemini_final_response
        "gemini_final_response.final_response": "final_response",

        # session_attributes
        "session_attributes.botname": "botName",
        "session_attributes.inputmode": "inputMode",
        "session_attributes.sessionid": "sessionid",
        "session_attributes.conversation_log": "conversation_log",
        "session_attributes.inputtranscript": "inputTranscript",

        # otros slots útiles (no en base_cols pero tal vez quieras guardarlos)
        "session_attributes.clavecliente": "clavecliente",
        "session_attributes.curp": "curp",
        "session_attributes.telefono": "telefono",
        "session_attributes.sucursal": "sucursal",
        "session_attributes.estado": "estado",
        "session_attributes.foliocita": "foliocita",
        "session_attributes.correo": "correo",
        "session_attributes.correoElectronico": "correoElectronico",
        "session_attributes.correo_WA": "correo_WA",
        "session_attributes.email": "email",
        "session_attributes.numeroCelular": "numeroCelular",
        "session_attributes.phoneNumber": "phoneNumber",
        "session_attributes.tel1": "tel1",
        "session_attributes.telefono1_WA": "telefono1_WA",
        "session_attributes.telefonos": "telefonos",
        "session_attributes.tels": "tels",
    }
    json_df = json_df.rename(columns=column_map)
    print(json_df.columns.tolist())
    df = df.drop(columns=["resource"]).join(resource_df)
    df = df.drop(columns=["jsonPayload"]).join(json_df)
    
    session_data = df[[col for col in base_cols if col in df.columns]].copy()

    # --- Mapeo y Creación de Nuevas Columnas ---
    session_data['gemini_intent_name'] = session_data.get('intent_name')
    session_data['gemini_knowledge_domain'] = session_data.get('knowledge_domain')
    session_data['gemini_origin_channel'] = session_data.get('origin_channel')
    session_data['gemini_origin'] = session_data.get('configuration_name')
    session_data['transacciondurantellamada'] = session_data.get('transactional_or_non_transactional')
    session_data['session_conversation_log'] = session_data.get('conversation_log', pd.Series(dtype=str)).fillna('') + \
                                            ', user_say: ' + \
                                            session_data.get('inputTranscript', pd.Series(dtype=str)).fillna('') + \
                                            ', bot_say: ' + \
                                            session_data.get('final_response', pd.Series(dtype=str)).fillna('')
    session_data['session_motivo_inicial'] = session_data.get('intent_name')
    session_data['session_id'] = session_data.get('sessionid')

    # Filtrar filas donde session_id es nulo
    session_data = session_data[session_data['session_id'].notna()]

    def determinar_canal(session_id):
        session_id_str = str(session_id)
        if 'whatsapp:' in session_id_str:
            return 'whatsapp'
        elif 'us-east-1' in session_id_str:
            return 'text'
        else:
            return 'speech'
    session_data['canal'] = session_data['session_id'].apply(determinar_canal)

    if 'slot_type' in session_data.columns:
        sessions_con_slot_type = session_data.dropna(subset=['slot_type'])['session_id'].unique()
        session_data.loc[session_data['session_id'].isin(sessions_con_slot_type), 'transacciondurantellamada'] = 'transactional'

    session_data = session_data.sort_values(by=['session_id', 'timestamp'])
    if 'intent_name' in session_data.columns:
        session_data['intent_name'] = session_data['intent_name'].replace('', np.nan)
        session_data['intent_name'] = session_data.groupby('session_id')['intent_name'].transform(lambda x: x.ffill().bfill())
        session_data['session_motivo_inicial'] = session_data['intent_name']
    if 'knowledge_domain' in session_data.columns:
        session_data['knowledge_domain'] = session_data['knowledge_domain'].replace('', np.nan)
        session_data['knowledge_domain'] = session_data.groupby('session_id')['knowledge_domain'].transform(lambda x: x.ffill().bfill())
        session_data['gemini_knowledge_domain'] = session_data['knowledge_domain']


    session_data['conversation_log_length'] = session_data['session_conversation_log'].str.len()
    session_data['has_intent'] = session_data['session_motivo_inicial'].notna().astype(int)

    representative_sessions = session_data.sort_values(
        by=['session_id', 'has_intent', 'conversation_log_length', 'timestamp'],
        ascending=[True, False, False, False]
    ).drop_duplicates('session_id', keep='first').copy()

    # Datos de contacto
    def get_contact_info(group):
        def find_first(data, cols):
            for col in cols:
                if col in data and not data[col].dropna().empty:
                    return data[col].dropna().iloc[0]
            return None

        curp_cols = ['curp']
        email_cols = ['correo', 'correoElectronico', 'correo_WA', 'email']
        phone_cols = ['telefono', 'numeroCelular', 'phoneNumber', 'tel1', 'telefono1_WA', 'telefonos', 'tels']

        curp = find_first(group, curp_cols)
        correo = find_first(group, email_cols)
        telefono = find_first(group, phone_cols)

        if not telefono and 'sessionid' in group:
            session_id_val = group['sessionid'].iloc[0]
            if isinstance(session_id_val, str) and session_id_val.startswith('whatsapp:'):
                telefono = session_id_val.replace('whatsapp:', '')

        return pd.Series({'curp': curp, 'correo': correo, 'telefono': telefono})

    #contact_info = df.groupby('sessionid').apply(get_contact_info, include_groups=False).reset_index()
    contact_info = df.groupby('sessionid').apply(get_contact_info).reset_index()


    session_times = session_data.groupby('session_id').agg(
        session_start=('timestamp', 'min'),
        session_end=('timestamp', 'max')
    ).reset_index()

    final_df = pd.merge(representative_sessions, session_times, on='session_id', how='left')
    final_df = pd.merge(final_df, contact_info, left_on='session_id', right_on='sessionid', how='left')

    def format_duration(td):
        total_seconds = int(td.total_seconds())
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{hours:02}:{minutes:02}:{seconds:02}"

    final_df['duracion'] = (final_df['session_end'] - final_df['session_start']).apply(format_duration)
    final_df['tiempo_por_sesion'] = final_df['duracion']
    final_df['horafinal'] = final_df['session_end'].dt.strftime('%H:%M:%S')
    final_df['year'] = final_df['session_start'].dt.year
    final_df['month'] = final_df['session_start'].dt.month


    # --- Estructura del DataFrame Final ---

    if 'timestamp' in final_df.columns:
        final_df = final_df.drop(columns=['timestamp'])

    DatosTemporales = final_df.rename(columns={
        'session_start': 'timestamp', 
        'session_id': 'sessionid',
        'gemini_origin': 'lineanegocio',
        'session_motivo_inicial': 'motivoinicial',
        'session_conversation_log': 'respuesta',
        'gemini_intent_name': 'nombretransaccion',
        'gemini_knowledge_domain': 'intentprevio',
    })

    # Añadir columnas calculadas/constantes
    DatosTemporales['concluyeenvoice'] = 'No'
    DatosTemporales['transferenciaasesor'] = DatosTemporales.get('nombretransaccion', pd.Series(dtype=str)).apply(
        lambda x: 'Si' if x == 'asesorEnLinea' else ''
    )
    DatosTemporales['datollave'] = None
    DatosTemporales['tramiteseleccionado'] = None
    DatosTemporales['tramiteaccion'] = None
    DatosTemporales['isfallback'] = None
    DatosTemporales['fallbackmessage'] = DatosTemporales.get('motivoinicial', pd.Series(dtype=str)).apply(
        lambda x: 'Yes' if x == 'FallbackIntent' else None
    )
    DatosTemporales['isderivacion'] = None
    DatosTemporales['__index_level_0__'] = '0'
    DatosTemporales['prestamoend'] = None
    DatosTemporales['flujoterminado'] = None

    # Seleccionar y reordenar las columnas finales
    final_columns = [
        'timestamp', 'sessionid', 'lineanegocio', 'motivoinicial', 'respuesta',
        'transacciondurantellamada', 'nombretransaccion', 'concluyeenvoice',
        'transferenciaasesor', 'datollave', 'canal', 'tramiteseleccionado',
        'tramiteaccion', 'intentprevio', 'isfallback', 'fallbackmessage',
        'isderivacion', 'duracion', 'tiempo_por_sesion', 'horafinal',
        '__index_level_0__', 'prestamoend', 'flujoterminado', 'curp',
        'correo', 'telefono', 'year', 'month'
    ]

    existing_columns = [col for col in final_columns if col in DatosTemporales.columns]
    DatosTemporales = DatosTemporales[existing_columns]


    DatosTemporales = DatosTemporales.sort_values(by='timestamp', ascending=False)

    return DatosTemporales