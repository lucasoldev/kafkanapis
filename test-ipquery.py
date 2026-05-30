import requests

def test_ipquery():
    """Testa a API IP Query IO."""
    url = "https://api.ipquery.io/?format=json"
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        print("✅ API IP Query IO respondendo com sucesso!")
        print(f"📡 Status Code: {response.status_code}")
        print(f"📦 Tipo de retorno: {type(data).__name__}")
        print(f"📦 Primeiros 200 caracteres do retorno:\n{str(data)[:200]}")
        
        return data
    except requests.exceptions.RequestException as e:
        print(f"❌ Erro ao consumir a API: {e}")
        return None

if __name__ == "__main__":
    test_ipquery()