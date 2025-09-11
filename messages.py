# messages.py

from typing import Optional

messages = {
    "en": {
        # --- Legacy CLI Menu (not used in Streamlit UI) ---
        "menu_header": "=== Expense Analyzer Menu ===",
        "menu_options": (
            "1: Add expense\n"
            "2: Show summary\n"
            "3: Filter expenses by date\n"
            "4: Check budget limits\n"
            "5: Set or update budget limits\n"
            "6: View all expenses\n"
            "7: Save & Exit\n"
            "8: Generate charts\n"
            "L: Change language\n"
        ),
        "enter_option": "Enter option: ",
        "invalid_option": "Invalid option! Please enter a number from 1 to 8.",
        # --- Streamlit UI: menu ---
        "menu.title": "Menu",
        "menu.dashboard": "Dashboard",
        "menu.add_expense": "Add Expense",
        "menu.browse": "Browse & Filter",
        "menu.charts": "Charts",
        "menu.settings": "Settings",
        # --- Streamlit UI: Add Expense ---
        "add_expense.category_mode": "Category",
        "add_expense.mode.existing": "Choose existing",
        "add_expense.mode.new": "Enter new",
        "add_expense.choose_existing": "Choose a category",
        "add_expense.new_category": "New category",
        # --- Streamlit UI: common labels ---
        "common.date": "Date",
        "common.amount": "Amount",
        "common.description": "Description",
        "common.submit": "Submit",
        # --- Input Prompts ---
        "enter_date": "Enter date (YYYY-MM-DD): ",
        "invalid_date": "Invalid date! Use YYYY-MM-DD.",
        "enter_category": "Enter category: ",
        "invalid_category": "Invalid category! Please try again.",
        "enter_amount": "Enter amount: ",
        "invalid_amount": "Invalid amount! Please enter a number.",
        "enter_description": "Enter description (optional): ",
        # --- Expense Actions ---
        "expense_summary": "Expense summary:",
        "expense_added": "Expense added successfully!",
        "no_expenses": "No expenses recorded.",
        "recent_expenses": "Recent expenses",
        # --- Summary & Reports ---
        "summary_header": "=== Expense Summary ===",
        "summary_line": "Category: {category}, Total: {total}",
        "total_expenses": "Total expenses: {total}",
        "note": "Note: {note}",
        # --- Budget ---
        "budget_header": "=== Budget Check ===",
        "budget_limit_updated": "Budget limit updated successfully!",
        "prompt_budget_limit_for_category": "Enter budget limit for category:",
        "enter_month": "Enter month (YYYY-MM): ",
        "budget_ok": "Within budget for {category}.",
        "budget_exceeded": "⚠️ Over budget for {category}!",
        "set_limit_category": "Enter category to set/update limit: ",
        "set_limit_amount": "Enter monthly limit for this category: ",
        "no_limits_set": "No budget limits have been set.",
        "limits_header": "=== Budget Limits ===",
        "limit_line": "Category: {category}, Limit: {limit}",
        # --- Filter ---
        "filter_start_date": "Enter start date (YYYY-MM-DD): ",
        "filter_end_date": "Enter end date (YYYY-MM-DD): ",
        "filter_prompt": "Enter start and end date (YYYY-MM-DD to YYYY-MM-DD): ",
        "filter_results_header": "=== Filtered Expenses ===",
        "no_results": "No expenses found for this period.",
        # --- Settings: Limits ---
        "month": "Month",
        "categories": "Categories",
        "edit_limits": "Edit Monthly Limits",
        "save": "Save",
        "clear_month": "Clear month limits",
        "saved": "Saved!",
        "cleared": "Cleared!",
        "download_csv": "Download CSV",
        "upload_csv": "Upload limits CSV",
        "csv_import_failed": "CSV import failed",
        "suggestions": "Suggestions (last 3 months)",
        "autofill_info": "Auto-filled this month's limits from history.",
        # --- Settings (limits / io / audit) ---
        "download_json": "Download JSON",
        "save_to_file": "Save to file",
        "clear_audit": "Clear audit",
        "import_success": "Limits imported from CSV",
        "change_log": "Change log (session)",
        "import_export": "Import / Export",
        # --- Exit ---
        "saving_data": "Saving data...",
        "goodbye": "Goodbye!",
        # ===== Reserved messages (not in use yet) =====
        "info_no_data_to_export": "No data to export.",
        "warning_enter_name": "Please enter a name.",
        "warning_user_name_exists": "User with this name already exists.",
        "info_cannot_delete_last_user": "You cannot delete the last remaining user.",
        "error_deletion_failed": "Deletion failed.",
        "warning_enter_new_name": "Please enter a new name.",
        "error_rename_failed": "Rename failed.",
        "caption_no_suggestions_yet": "No suggestions yet.",
    },
    "fr": {
        # --- Legacy CLI Menu (not used in Streamlit UI) ---
        "menu_header": "=== Menu de l'Analyseur de Dépenses ===",
        "menu_options": (
            "1: Ajouter une dépense\n"
            "2: Voir le résumé\n"
            "3: Filtrer les dépenses par date\n"
            "4: Vérifier les limites budgétaires\n"
            "5: Définir ou mettre à jour les limites budgétaires\n"
            "6: Voir toutes les dépenses\n"
            "7: Sauvegarder et Quitter\n"
            "8: Générer des graphiques\n"
            "L: Changer de langue\n"
        ),
        "enter_option": "Choisissez une option : ",
        "invalid_option": "Option invalide ! Veuillez entrer un nombre de 1 à 8.",
        # --- Interface Streamlit : menu ---
        "menu.title": "Menu",
        "menu.dashboard": "Tableau de bord",
        "menu.add_expense": "Ajouter une dépense",
        "menu.browse": "Parcourir & Filtrer",
        "menu.charts": "Graphiques",
        "menu.settings": "Paramètres",
        # --- Interface Streamlit : Ajouter une dépense ---
        "add_expense.category_mode": "Catégorie",
        "add_expense.mode.existing": "Choisir existante",
        "add_expense.mode.new": "Saisir nouvelle",
        "add_expense.choose_existing": "Choisissez une catégorie",
        "add_expense.new_category": "Nouvelle catégorie",
        # --- Interface Streamlit : libellés communs ---
        "common.date": "Date",
        "common.amount": "Montant",
        "common.description": "Description",
        "common.submit": "Valider",
        # --- Input Prompts ---
        "enter_date": "Entrez la date (AAAA-MM-JJ) : ",
        "invalid_date": "Date invalide ! Utilisez AAAA-MM-JJ.",
        "enter_category": "Entrez la catégorie : ",
        "invalid_category": "Catégorie invalide ! Veuillez réessayer.",
        "enter_amount": "Entrez le montant : ",
        "invalid_amount": "Montant invalide ! Veuillez entrer un nombre.",
        "enter_description": "Entrez une description (optionnel) : ",
        # --- Expense Actions ---
        "expense_summary": "Résumé des dépenses :",
        "expense_added": "Dépense ajoutée avec succès !",
        "no_expenses": "Aucune dépense enregistrée.",
        "recent_expenses": "Dépenses récentes",
        # --- Summary & Reports ---
        "summary_header": "=== Résumé des Dépenses ===",
        "summary_line": "Catégorie : {category}, Total : {total}",
        "total_expenses": "Dépenses totales : {total}",
        "note": "Note : {note}",
        # --- Budget ---
        "budget_header": "=== Vérification du Budget ===",
        "budget_limit_updated": "Limite budgétaire mise à jour avec succès !",
        "prompt_budget_limit_for_category": "Entrez la limite budgétaire pour la catégorie :",
        "enter_month": "Entrez le mois (AAAA-MM) : ",
        "budget_ok": "Dans le budget pour {category}.",
        "budget_exceeded": "⚠️ Dépassement du budget pour {category} !",
        "set_limit_category": "Entrez la catégorie pour définir/mettre à jour la limite : ",
        "set_limit_amount": "Entrez la limite mensuelle pour cette catégorie : ",
        "no_limits_set": "Aucune limite budgétaire n’a été définie.",
        "limits_header": "=== Limites Budgétaires ===",
        "limit_line": "Catégorie : {category}, Limite : {limit}",
        # --- Filter ---
        "filter_start_date": "Entrez la date de début (AAAA-MM-JJ) : ",
        "filter_end_date": "Entrez la date de fin (AAAA-MM-JJ) : ",
        "filter_prompt": "Entrez la date de début et de fin (AAAA-MM-JJ à AAAA-MM-JJ) : ",
        "filter_results_header": "=== Dépenses Filtrées ===",
        "no_results": "Aucune dépense trouvée pour cette période.",
        # --- Settings: Limits ---
        "month": "Mois",
        "categories": "Catégories",
        "edit_limits": "Modifier les plafonds mensuels",
        "save": "Enregistrer",
        "clear_month": "Effacer les plafonds du mois",
        "saved": "Enregistré !",
        "cleared": "Effacé !",
        "download_csv": "Télécharger CSV",
        "upload_csv": "Importer des plafonds (CSV)",
        "csv_import_failed": "Échec de l'import CSV",
        "suggestions": "Suggestions (3 derniers mois)",
        "autofill_info": "Plafonds de ce mois auto-remplis à partir de l'historique.",
        # --- Settings (limits / io / audit) ---
        "download_json": "Télécharger JSON",
        "save_to_file": "Enregistrer dans un fichier",
        "clear_audit": "Effacer l'audit",
        "import_success": "Plafonds importés depuis le CSV",
        "change_log": "Journal des modifications (session)",
        "import_export": "Importer / Exporter",
        # --- Exit ---
        "saving_data": "Sauvegarde des données...",
        "goodbye": "Au revoir !",
        # --- Reserved messages (not in use yet) ---
        "info_no_data_to_export": "Aucune donnée à exporter.",
        "warning_enter_name": "Veuillez entrer un nom.",
        "warning_user_name_exists": "Un utilisateur avec ce nom existe déjà.",
        "info_cannot_delete_last_user": "Vous ne pouvez pas supprimer le dernier utilisateur restant.",
        "error_deletion_failed": "Échec de la suppression.",
        "warning_enter_new_name": "Veuillez entrer un nouveau nom.",
        "error_rename_failed": "Échec du renommage.",
        "caption_no_suggestions_yet": "Pas encore de suggestions.",
    },
    "es": {
        # --- Legacy CLI Menu (not used in Streamlit UI) ---
        "menu_header": "=== Menú del Analizador de Gastos ===",
        "menu_options": (
            "1: Añadir gasto\n"
            "2: Ver resumen\n"
            "3: Filtrar gastos por fecha\n"
            "4: Revisar límites de presupuesto\n"
            "5: Establecer o actualizar límites de presupuesto\n"
            "6: Ver todos los gastos\n"
            "7: Guardar y Salir\n"
            "8: Generar gráficos\n"
            "L: Cambiar idioma\n"
        ),
        "enter_option": "Elija una opción: ",
        "invalid_option": "¡Opción inválida! Por favor, ingrese un número del 1 al 8.",
        # --- Interfaz Streamlit: menú ---
        "menu.title": "Menú",
        "menu.dashboard": "Panel",
        "menu.add_expense": "Añadir gasto",
        "menu.browse": "Buscar & Filtrar",
        "menu.charts": "Gráficos",
        "menu.settings": "Ajustes",
        # --- Interfaz Streamlit: Añadir gasto ---
        "add_expense.category_mode": "Categoría",
        "add_expense.mode.existing": "Elegir existente",
        "add_expense.mode.new": "Ingresar nueva",
        "add_expense.choose_existing": "Elige una categoría",
        "add_expense.new_category": "Nueva categoría",
        # --- Interfaz Streamlit: etiquetas comunes ---
        "common.date": "Fecha",
        "common.amount": "Importe",
        "common.description": "Descripción",
        "common.submit": "Enviar",
        # --- Input Prompts ---
        "enter_date": "Ingrese la fecha (AAAA-MM-DD): ",
        "invalid_date": "¡Fecha inválida! Use AAAA-MM-DD.",
        "enter_category": "Ingrese la categoría: ",
        "invalid_category": "¡Categoría inválida! Intente de nuevo.",
        "enter_amount": "Ingrese el monto: ",
        "invalid_amount": "¡Monto inválido! Por favor, ingrese un número.",
        "enter_description": "Ingrese una descripción (opcional): ",
        # --- Expense Actions ---
        "expense_summary": "Resumen de gastos:",
        "expense_added": "¡Gasto agregado con éxito!",
        "no_expenses": "No se registraron gastos.",
        "recent_expenses": "Gastos recientes",
        # --- Summary & Reports ---
        "summary_header": "=== Resumen de Gastos ===",
        "summary_line": "Categoría: {category}, Total: {total}",
        "total_expenses": "Gastos totales: {total}",
        "note": "Nota: {note}",
        # --- Budget ---
        "budget_header": "=== Revisión del Presupuesto ===",
        "budget_limit_updated": "¡Límite de presupuesto actualizado con éxito!",
        "prompt_budget_limit_for_category": "Ingrese el límite de presupuesto para la categoría:",
        "enter_month": "Ingrese el mes (AAAA-MM): ",
        "budget_ok": "Dentro del presupuesto para {category}.",
        "budget_exceeded": "⚠️ ¡Presupuesto excedido para {category}!",
        "set_limit_category": "Ingrese la categoría para establecer/actualizar el límite: ",
        "set_limit_amount": "Ingrese el límite mensual para esta categoría: ",
        "no_limits_set": "No se han establecido límites de presupuesto.",
        "limits_header": "=== Límites de Presupuesto ===",
        "limit_line": "Categoría: {category}, Límite: {limit}",
        # --- Filter ---
        "filter_start_date": "Ingrese la fecha de inicio (AAAA-MM-DD): ",
        "filter_end_date": "Ingrese la fecha de fin (AAAA-MM-DD): ",
        "filter_prompt": "Ingrese la fecha de inicio y de fin (AAAA-MM-DD a AAAA-MM-DD): ",
        "filter_results_header": "=== Gastos Filtrados ===",
        "no_results": "No se encontraron gastos para este período.",
        # --- Settings: Limits ---
        "month": "Mes",
        "categories": "Categorías",
        "edit_limits": "Editar límites mensuales",
        "save": "Guardar",
        "clear_month": "Limpiar límites del mes",
        "saved": "¡Guardado!",
        "cleared": "¡Limpiado!",
        "download_csv": "Descargar CSV",
        "upload_csv": "Importar límites (CSV)",
        "csv_import_failed": "Error al importar CSV",
        "suggestions": "Sugerencias (últimos 3 meses)",
        "autofill_info": "Límites de este mes auto-rellenados desde el historial.",
        # --- Settings (limits / io / audit) ---
        "download_json": "Descargar JSON",
        "save_to_file": "Guardar en archivo",
        "clear_audit": "Borrar auditoría",
        "import_success": "Límites importados desde CSV",
        "change_log": "Registro de cambios (sesión)",
        "import_export": "Importar / Exportar",
        # --- Exit ---
        "saving_data": "Guardando datos...",
        "goodbye": "¡Adiós!",
        # ===== Reserved messages (not in use yet) =====
        "info_no_data_to_export": "No hay datos para exportar.",
        "warning_enter_name": "Por favor, ingrese un nombre.",
        "warning_user_name_exists": "Ya existe un usuario con este nombre.",
        "info_cannot_delete_last_user": "No puede eliminar el último usuario restante.",
        "error_deletion_failed": "Error en la eliminación.",
        "warning_enter_new_name": "Por favor, ingrese un nuevo nombre.",
        "error_rename_failed": "Error al renombrar.",
        "caption_no_suggestions_yet": "Aún no hay sugerencias.",
    },
}

# --- aliases ---
ALIASES = {
    "limit_updated": "budget_limit_updated",
    # совместимость со старыми/жёсткими ключами:
    "Settings": "menu.settings",
    "settings": "menu.settings",
    # избавляемся от дубликатов между меню и заголовком страницы
    "add_expense.title": "menu.add_expense",
}


def _resolve_alias(key: str) -> str:
    seen = set()
    cur = key
    while cur in ALIASES:
        if cur in seen:
            break
        seen.add(cur)
        cur = ALIASES[cur]
    return cur


def t(key: str, lang: str, default: Optional[str] = None) -> str:
    """Возвращает перевод ключа key для языка lang.
    Если ключ/перевод отсутствует или пуст, возвращает default (или сам key, если default=None).
    """
    # алиасы
    real_key = ALIASES.get(key, key)

    # словарь языка
    lang_dict = messages.get(lang, {})

    # извлекаем перевод
    val = lang_dict.get(real_key)
    if val is None or (isinstance(val, str) and val.strip() == ""):
        return default if default is not None else real_key
    return val
