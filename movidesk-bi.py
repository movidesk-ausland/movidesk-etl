import requests
import pandas as pd
import os
import json
import numpy as np
from supabase import create_client

# ============================================================
# CREDENCIAIS SUPABASE
# ============================================================
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://uraxoruvcyggxubdpbar.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InVyYXhvcnV2Y3lnZ3h1YmRwYmFyIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzQyNzg0OTIsImV4cCI6MjA4OTg1NDQ5Mn0.I1H1IUMZ6nnGgMPdHqjlZjypelXh85A2jTPYdLaT4J0")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ============================================================
# COLETA DE DADOS DA API MOVIDESK
# ============================================================
def fetch_data_from_api(url, params):
    all_data = []
    page = 0

    while True:
        print(f"Buscando dados da pagina {page + 1}...")
        params["$skip"] = page * params["$top"]
        response = requests.get(url, params=params)

        if response.status_code == 200:
            data = response.json()
            if not data:
                print("Todas as paginas foram carregadas.")
                break
            all_data.extend(data)
            page += 1
        else:
            print("Erro ao acessar a API:", response.status_code)
            print(response.text)
            break

    return all_data

url_1 = "https://api.movidesk.com/public/v1/tickets/past"
url_2 = "https://api.movidesk.com/public/v1/tickets"

params_1 = {
    "token": "a6ad3ac4-e949-4f3b-8c76-9fe84cbcee2f",
    "$select": "id,subject,serviceFull,createdDate,lastActionDate,owner,ownerTeam,baseStatus,status,category,clients,origin,actions",
    "$expand": "actions($select=origin,createdDate),clients($select=businessName;$expand=organization($select=businessName)),owner($select=businessName)",
    "$filter": "baseStatus ne 'Canceled' and createdDate gt 2021-09-01T00:00:00.00z",
    "$top": 1000,
    "$skip": 0
}

params_2 = params_1.copy()

data_1 = fetch_data_from_api(url_1, params_1)
data_2 = fetch_data_from_api(url_2, params_2)
all_data = data_1 + data_2

# ============================================================
# PROCESSAMENTO DOS DADOS
# ============================================================
df = pd.DataFrame(all_data)

df['serviceFull'] = df['serviceFull'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
df['ownerTeam'] = df['ownerTeam'].apply(lambda x: x if isinstance(x, str) else None)
df['owner'] = df['owner'].apply(lambda x: x['businessName'] if isinstance(x, dict) and 'businessName' in x else None)

if 'clients' in df.columns:
    df_exploded = df.explode('clients')
    df_clients = pd.json_normalize(df_exploded['clients'].dropna())
    df_clients = df_clients.rename(columns={'organization.businessName': 'clientOrganizationName'})
    df_clients['clientOrganizationName'] = df_clients['clientOrganizationName'].fillna(df_clients['businessName'])
    df_clients = df_clients.drop(columns=['organization'], errors='ignore')
    df = df_exploded.drop(columns=['clients'], errors='ignore').reset_index(drop=True)
    df = pd.concat([df, df_clients.reset_index(drop=True)], axis=1)
else:
    print("A coluna 'clients' nao esta presente no DataFrame.")

def flatten_actions(ticket):
    flat_ticket = ticket.copy()
    actions = flat_ticket.pop("actions", [])
    flat_ticket['origin'] = flat_ticket.get("origin")
    flat_ticket['createdDate'] = flat_ticket.get("createdDate")
    flat_ticket['inicio_atendimento'] = None

    if actions:
        actions_sorted = sorted(actions, key=lambda x: x.get("createdDate"))
        for action in actions_sorted:
            if action.get("origin") == 2:
                flat_ticket['inicio_atendimento'] = action.get("createdDate")
                break

    return flat_ticket

df = pd.DataFrame([flatten_actions(ticket) for ticket in df.to_dict(orient='records')])

def remove_t_from_datetime(value):
    if isinstance(value, str):
        return value.replace('T', ' ')
    return value

df['inicio_atendimento'] = df['inicio_atendimento'].apply(remove_t_from_datetime)
for col in df.columns:
    if 'Date' in col:
        df[col] = df[col].apply(remove_t_from_datetime)

df['id'] = pd.to_numeric(df['id'], errors='coerce')
df = df.drop_duplicates(subset='id', keep='first')

origin_mapping = {
    1: "Via web pelo cliente",
    2: "Via web pelo agente",
    3: "Recebido via email",
    5: "Chat (online)",
    6: "Mensagem offline",
    7: "Email enviado pelo sistema",
    13: "Ligacao Recebida",
    16: "DropoutCall",
    25: "WhatsApp Ativo",
}

df['origin_desc'] = df['origin'].map(origin_mapping)
df = df[df['origin'] != 16]
df = df[~((df['inicio_atendimento'].isna()) & (df['status'] == 'Fechado'))]

# ============================================================
# ENVIO PARA O SUPABASE
# ============================================================
df = df.rename(columns={
    "serviceFull":            "service_full",
    "createdDate":            "created_date",
    "lastActionDate":         "last_action",
    "ownerTeam":              "owner_team",
    "baseStatus":             "base_status",
    "clientOrganizationName": "client_name",
    "inicio_atendimento":     "inicio_atend",
})

colunas = [
    "id", "subject", "service_full", "created_date", "last_action",
    "owner", "owner_team", "base_status", "status",
    "category", "origin", "origin_desc", "client_name", "inicio_atend"
]

colunas_existentes = [c for c in colunas if c in df.columns]
df_final = df[colunas_existentes].copy()
df_final['id'] = df_final['id'].astype(int)

# Eliminar NaN e Infinity convertendo via JSON
df_final = df_final.replace([np.inf, -np.inf], None)
df_final = df_final.where(df_final.notna(), None)
records = json.loads(df_final.to_json(orient="records", force_ascii=False))

print(f"\nTotal de registros a enviar: {len(records)}")

BATCH = 500
for i in range(0, len(records), BATCH):
    lote = records[i:i + BATCH]
    supabase.table("tickets").upsert(lote, on_conflict="id").execute()
    print(f"Enviados registros {i + 1} a {i + len(lote)}")

print("\nBanco de dados atualizado com sucesso!")
