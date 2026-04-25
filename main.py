from src.backend.api import API, app, process_inventory


def main() -> None:
    """
    Executa o pipeline completo:
    1. Busca dados da API externa.
    2. Salva o arquivo raw_data.json.
    3. Processa o inventário.
    4. Imprime as métricas calculadas.
    """
    api = API()

    df = api.fetch_data()

    print("Primeiros 5 registros do DataFrame bruto:")
    print(df.head())

    metrics = process_inventory("raw_data.json")

    print("Métricas calculadas do inventário:")
    print(metrics)


if __name__ == "__main__":
    main()