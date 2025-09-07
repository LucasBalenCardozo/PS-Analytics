import requests

def _default_params(offset: int = 0, limit: int = 2000):
    """
    Parâmetros canônicos para o endpoint de ArcGIS REST.
    """
    return {
        "where": "1=1",                 # obrigatório para retornar tudo
        "outFields": "*",               # pegar todas as colunas (além da geometria)
        "returnGeometry": "true",       # manter geometria (processaremos no passo seguinte)
        "outSR": "4326",                # coordenadas em WGS84 (lat/long)
        "f": "geojson",                    # formato de resposta
        "resultOffset": offset,
        "resultRecordCount": limit      # tamanho da página (paginação manual)
    }


def fetch_page(session: requests.Session, url = str, offset: int = 0, extra_params: dict() = None, limit: int = 2000):
    """
    Busca uma página do serviço com resultOffset.
    Retorna o dicionário JSON da resposta do ArcGIS.
    """
    params = _default_params(offset, limit)

    if extra_params:
        params.update(extra_params)

    resp = session.get(url, params=params, timeout=60)
    resp.raise_for_status()

    data = resp.json()
    
    # Erros do ArcGIS vêm dentro do próprio JSON
    if "error" in data:
        raise RuntimeError(f"ArcGIS error: {data['error']}")
        
    return data


def fetch_all_features(extra_params: dict() = None, verbose: bool = True):
    """
    Busca todas as páginas e concatena a lista de 'features' em memória.
    Usa o indicador 'exceededTransferLimit' para saber se deve continuar paginando.
    """
    features = []
    offset = 0

    ARCGIS_URL = "https://sigel.aneel.gov.br/arcgis/rest/services/PORTAL/WFS/MapServer/0/query"
    
    # Dica: o serviço normalmente limita ~1000 por requisição. Usaremos esse chunk.
    limit = 1000

    with requests.Session() as s:
        while True:
            data = fetch_page(s, url=ARCGIS_URL, offset=offset, extra_params=extra_params, limit = limit)
            page_features = data.get("features", [])
            features.extend(page_features)

            if verbose:
                print(f"[INFO] Página offset={offset} → {len(page_features)} registros (acum: {len(features)})")

            # Sinal do ArcGIS de que ainda há mais páginas
            exceeded = data.get("exceededTransferLimit", False)

            # Heurística adicional: se veio menos que o limit, geralmente acabou
            if not exceeded or len(page_features) < limit:
                break

            offset += limit

    return features

