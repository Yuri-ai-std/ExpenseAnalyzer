# messages.py

messages = {
    "en": {
        # --- Menu ---
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
        "limit_updated": "Budget limit updated successfully!",
        "no_limits_set": "No budget limits have been set.",
        "limits_header": "=== Budget Limits ===",
        "limit_line": "Category: {category}, Limit: {limit}",
        # --- Filter ---
        "filter_start_date": "Enter start date (YYYY-MM-DD): ",
        "filter_end_date": "Enter end date (YYYY-MM-DD): ",
        "filter_prompt": "Enter start and end date (YYYY-MM-DD to YYYY-MM-DD): ",
        "filter_results_header": "=== Filtered Expenses ===",
        "no_results": "No expenses found for this period.",
        # --- Exit ---
        "saving_data": "Saving data...",
        "goodbye": "Goodbye!",
    },
    "fr": {
        # --- Menu ---
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
        "limit_updated": "Limite budgétaire mise à jour avec succès !",
        "no_limits_set": "Aucune limite budgétaire n’a été définie.",
        "limits_header": "=== Limites Budgétaires ===",
        "limit_line": "Catégorie : {category}, Limite : {limit}",
        # --- Filter ---
        "filter_start_date": "Entrez la date de début (AAAA-MM-JJ) : ",
        "filter_end_date": "Entrez la date de fin (AAAA-MM-JJ) : ",
        "filter_prompt": "Entrez la date de début et de fin (AAAA-MM-JJ à AAAA-MM-JJ) : ",
        "filter_results_header": "=== Dépenses Filtrées ===",
        "no_results": "Aucune dépense trouvée pour cette période.",
        # --- Exit ---
        "saving_data": "Sauvegarde des données...",
        "goodbye": "Au revoir !",
    },
    "es": {
        # --- Menu ---
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
        "limit_updated": "¡Límite de presupuesto actualizado con éxito!",
        "no_limits_set": "No se han establecido límites de presupuesto.",
        "limits_header": "=== Límites de Presupuesto ===",
        "limit_line": "Categoría: {category}, Límite: {limit}",
        # --- Filter ---
        "filter_start_date": "Ingrese la fecha de inicio (AAAA-MM-DD): ",
        "filter_end_date": "Ingrese la fecha de fin (AAAA-MM-DD): ",
        "filter_prompt": "Ingrese la fecha de inicio y de fin (AAAA-MM-DD a AAAA-MM-DD): ",
        "filter_results_header": "=== Gastos Filtrados ===",
        "no_results": "No se encontraron gastos para este período.",
        # --- Exit ---
        "saving_data": "Guardando datos...",
        "goodbye": "¡Adiós!",
    },
}
