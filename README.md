# ExpenseAnalyzer

**Author:** Yuri Oshurko  
**Location:** Montreal, Canada  

ExpenseAnalyzer is a Python-based CLI application for tracking personal expenses, managing budgets, and analyzing spending habits.  
Originally developed as a **final project for Harvard CS50P**, the tool has evolved with new features, database support, and plans for future enhancements.

---

## Features

### 1. Expense Management
- **Add expenses** with date, category, amount, and description.
- **View expenses** with detailed breakdowns.
- **Filter expenses by date range** for focused analysis.
- **Summarize expenses by category** for clear overviews.

### 2. Budget Tracking
- **Set monthly budget limits** per category.
- **Check budget usage** and receive alerts when nearing or exceeding limits.
- **Monthly budget reports** when filtering by date.

### 3. Data Storage
- **SQLite database support** for reliable and structured storage.
- Automatic database creation on first run.
- Simple switch between JSON and SQLite via configuration.

### 4. Exporting Data
- **Export expenses to CSV** for use in Excel or Google Sheets.

### 5. Multilingual Support
- Interface available in:
  - English
  - French
  - Spanish

---

## Installation

```bash
# Clone the repository
git clone https://github.com/Yuri-ai-std/ExpenseAnalyzer.git

# Navigate to the project folder
cd ExpenseAnalyzer

# (Optional) Create a virtual environment
python3 -m venv venv
source venv/bin/activate  # Mac/Linux
venv\Scripts\activate     # Windows

# Install dependencies
pip install -r requirements.txt

Usage

Run the program: 
python3 project.py

Main Menu Options
	1.	Add an expense
	2.	View expenses
	3.	Filter expenses by date
	4.	Summarize expenses by category
	5.	Check budget limits
	6.	Export expenses to CSV
	7.	Exit

Example Output

=== Expense Summary ===
Food: $120.50
Transport: $45.00
Entertainment: $30.00
Total: $195.50

Project Structure

ExpenseAnalyzer_Restore/
│
├── .gitignore               # Files and folders to ignore in Git
├── budget_limits.json       # Stores monthly budget limits (JSON format)
├── db.py                    # SQLite database integration functions
├── expenses.db              # SQLite database file
├── expenses.json            # Expenses data (JSON format, optional if using SQLite)
├── messages.py              # Multi-language messages and prompts
├── project.py               # Main application file (CLI interface)
├── README.md                # Project documentation
├── requirements.txt         # Python dependencies
├── test_project.py          # Automated tests (pytest)
├── utils.py                 # Utility functions (e.g., date parsing, validation)
└── .pytest_cache/           # Pytest cache folder

Testing

This project uses pytest for automated testing.
python3 -m pytest test_project.py

Future Development
	•	Streamlit web interface for a modern, user-friendly UI.
	•	Graphical spending analysis with charts.
	•	Multi-user accounts with authentication.
	•	Automatic monthly budget resets.
	•	Cloud sync for access from multiple devices.

⸻

License

This project is licensed under the MIT License.
See the LICENSE file for details.

Created with dedication and continuous improvement in mind.