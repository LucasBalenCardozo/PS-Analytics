"""
Data Processing — Casa dos Ventos (PS Analytics)
Lê o GEOJSON bruto (features do ArcGIS), cria um GeoDataFrame (WGS84),
extrai latitude/longitude e exporta um .csv para uso no Tableau.

Entradas:
- geojson resultado da API   (gerado no passo 1)

Saídas:
- outputs/aerogeradores.csv            (com colunas de atributos + latitude/longitude)
"""

import json
import geopandas as gpd

def gdf_from_geojson(geojson: dict):
    """
    Converte a resposta da API (GeoJSON ou ESRI JSON) em GeoDataFrame no CRS EPSG:4326.
    """
    gdf = gpd.GeoDataFrame.from_features(geojson, crs="EPSG:4326")
    # garante CRS correto
    if gdf.crs is None:
        gdf.set_crs("EPSG:4326", inplace=True)
    elif gdf.crs.to_string() != "EPSG:4326":
        gdf = gdf.to_crs("EPSG:4326")
    return gdf

def lowercase_columns(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Converte todos os nomes de colunas de um GeoDataFrame para minúsculo.
    Mantém a coluna 'geometry' inalterada.
    """
    gdf = gdf.copy()
    gdf.columns = [col.lower() if col != gdf.geometry.name else col for col in gdf.columns]
    return gdf

def add_lat_lon(gdf: gpd.GeoDataFrame):
    """
    Cria colunas 'latitude' e 'longitude' a partir da coluna de geometria (Point).
    Assume CRS WGS84 (EPSG:4326), onde geometry.x = lon e geometry.y = lat.
    """
    # Se for Point: .x (lon), .y (lat)
    gdf["longitude"] = gdf.geometry.x
    gdf["latitude"] = gdf.geometry.y
    return gdf

def date_to_utc(gdf: gpd.GeoDataFrame):
    """
    Cria coluna com a data de atualização no padrão UTC.
    """
    date_cols=["data_atualizacao"]
    
    for column in date_cols:
        if column in gdf.columns:
            gdf[f"{column}_utc"] = gdf[column].astype("datetime64[ms]").dt.tz_localize("UTC")
            gdf.drop(columns=[column], inplace=True)

    return gdf
    
def deal_with_line_breaks(gdf: gpd.GeoDataFrame):
    """
    Lida com quebras de linha existentes nos dados para não dar erro na hora de gerar o aquivo csv.
    """
    for col in gdf.select_dtypes(include="object").columns:
        gdf[col] = gdf[col].str.replace(r'[\r\n]+', ' ', regex=True)

    return gdf

def remove_outliers_iqr(gdf: gpd.GeoDataFrame,columns: list,k: float = 1.5, treat_na_as_outlier: bool = True):
    """
    Remove linhas de um GeoDataFrame que contenham outliers (método Boxplot/IQR)
    em qualquer uma das colunas especificadas.

    Parâmetros
    ----------
    gdf : gpd.GeoDataFrame
        GeoDataFrame de entrada.
    columns : list
        Lista com os nomes das colunas numéricas para aplicar o método IQR.
    k : float, opcional (default=1.5)
        Fator multiplicativo do IQR para definir os limites (1.5 é o padrão do boxplot).
    treat_na_as_outlier : bool, opcional (default=False)
        Se True, linhas com NaN nessas colunas serão removidas.
        Se False, NaN não contam como outlier (são mantidos).

    Retorno
    -------
    gpd.GeoDataFrame
        Novo GeoDataFrame sem as linhas consideradas outliers.

    Observações
    -----------
    - Outliers são definidos como valores < Q1 - k*IQR ou > Q3 + k*IQR.
    - Se a coluna tiver IQR == 0 (constante), ela não remove nada.
    - Linhas são removidas se QUALQUER coluna indicada tiver outlier.
    """

    # Cria uma cópia para não modificar o original
    gdf_filtered = gdf.copy()
    data_count = len(gdf_filtered)
    
    # Começa mantendo todas as linhas
    keep_mask = gdf_filtered.index == gdf_filtered.index

    for col in columns:
        if col not in gdf_filtered.columns:
            raise KeyError(f"Column '{col}' not found in GeoDataFrame.")

        series = gdf_filtered[col]

        # Garante que é numérica (sem importar pandas explicitamente)
        if not hasattr(series.dtype, "kind") or series.dtype.kind not in ("i", "u", "f"):
            raise TypeError(f"Column '{col}' is not numeric. Convert before applying IQR.")

        q1 = series.quantile(0.25)
        q3 = series.quantile(0.75)
        iqr = q3 - q1

        lower_limit = q1 - k * iqr
        upper_limit = q3 + k * iqr

        col_inlier = (series >= lower_limit) & (series <= upper_limit)

        if treat_na_as_outlier:
            # NaN contam como outlier -> mantem apenas True
            col_mask = col_inlier.fillna(False)
        else:
            # NaN não contam como outlier -> NaN tratados como True (mantém linha)
            col_mask = col_inlier.fillna(True)

        # Mantém apenas linhas que não são outlier nesta coluna
        keep_mask = keep_mask & col_mask

        gdf_filtered = gdf_filtered[keep_mask].reset_index(drop=True)
        
        if len(gdf_filtered) < data_count:
                print(f"[INFO] Detecção e remoção de {data_count - len(gdf_filtered)} outlier(s) na coluna {col}")
                data_count = len(gdf_filtered)

    return gdf_filtered

def validate_gdf(gdf: gpd.GeoDataFrame):

    """
    Aplica validações básicas:
      - remove geometrias nulas/invalidas,
      - filtra versão para usar apenas versão válida,
      - filtra e gera warning sobre coluna operação,
      - exclui linhas com valores nulos,
      - detecção de outlier na coluna pot_wm,
      - exclusão de colunas não úteis nas visualização.
    """
    
    data_removed = dict()

    initial_count = len(gdf)
    print(f"[INFO] Número de linhas dos dados crus: {initial_count}")
    
    # 1) Geometrias válidas
    gdf = gdf[gdf.geometry.notna()].copy()
    gdf = gdf[gdf.geometry.is_valid]

    valid_geometry_dealt_count = len(gdf)
    print(f"[INFO] Número de linhas com geometria válida: {valid_geometry_dealt_count}")

    # 2) Filtros nas colunas

    # 2.1) Somente versão válida
    
    gdf = gdf[gdf["versao"] == "Versão Válida"]

    version_dealt_count = len(gdf)
    print(f"[INFO] Número de linhas com versão válida: {version_dealt_count}")

    # 2.2) Aerogeradores em operação

    gdf["operacao"] = gdf["operacao"].replace([1, "1"], "Sim") # foi considerado 1 == sim

    missing_operation_info = gdf["operacao"].isnull().sum()

    if missing_operation_info > 0: 
        print(f"[WARNING] Número de linhas com informação de operação nula excluídas dos dados: {missing_operation_info}")

    gdf = gdf.dropna(subset=["operacao"])    
    operation_dealt_count = len(gdf)
    print(f"[INFO] Número de linhas após exclusão de dados sem informação de operação: {operation_dealt_count}")

    # 2.3) Excluir dados com nulo em determinada coluna

    drop_na_columns = ["pot_mw", "alt_total", "alt_torre", "diam_rotor", "eol_versao_id", "nome_eol", "den_aeg"]
    gdf = gdf.dropna(subset=drop_na_columns)

    na_values_dealt_count = len(gdf)

    print(f"[INFO] Número de linhas removendo linhas com campos vazios nos dados: {na_values_dealt_count}")
    
    # 3) Detecção de outlier em colunas
    
    check_outliers = ["pot_mw"]
    gdf = remove_outliers_iqr(gdf, check_outliers, k=3)

    # 4) Excluir dados em que latitude e longitude são iguais 
    
    gdf = gdf.drop_duplicates(subset=["latitude", "longitude"]).reset_index(drop=True)
    duplicates_dealt_count = len(gdf)
    print(f"[INFO] Número de linhas removendo aerogeradores duplicados nos dados: {duplicates_dealt_count}")

    # 5) Dropar colunas que não serão utilizadas 
    
    drop_columns = ["geometry", "origem", "x", "y", "datum_emp", "fuso_ag", "versao"]
    gdf.drop(columns=drop_columns, inplace=True)
    
    return gdf
