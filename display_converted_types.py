from type_mapping import convert_type

def afficher_colonnes_converties(schema_data):
    """
    Affiche pour chaque table les colonnes avec leurs types PostgreSQL d'origine
    et le type converti en Oracle, en format tabulaire clair.
    """

    for table_name, table_info in schema_data["tables"].items():
        columns = table_info.get("columns", [])
        print(f"\n--- Table: {table_name} ({len(columns)} colonnes) ---")
        print(f"{'Colonne':<25} {'Type PostgreSQL':<35} {'Type Oracle converti'}")
        print("=" * 75)

        for col in columns:
            col_name = col[0]
            pg_type = col[1]

            # Gestion des types spécifiques avec précision ou longueur
            if pg_type == 'character varying':
                char_length = col[2]
                pg_type_formatted = f'{pg_type}({char_length})' if char_length else pg_type
            elif pg_type == 'numeric':
                # Ici, gérer precision et scale si disponibles dans col (adapter selon extraction)
                pg_type_formatted = pg_type  # Ajuster si info précision/scale extraite
            else:
                pg_type_formatted = pg_type

            or_type = convert_type(pg_type_formatted)
            print(f"{col_name:<25} {pg_type_formatted:<35} {or_type}")

        print("-" * 75)
