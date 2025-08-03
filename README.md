# README — ExpenseAnalyzer

📛 **Project Title**  
ExpenseAnalyzer

📝 **Description**  
ExpenseAnalyzer is a simple and intuitive tool for tracking personal expenses.  
It helps you control your budget, get smart recommendations for financial optimization, and use the app in your native language.  
We included support for English, French, and Spanish to ensure a smooth experience for users across North America.  
The interface runs in the terminal and requires no special skills.

🎯 **Project Goals**
- Help people manage their spending better  
- Notify when the budget is exceeded in any category  
- Support three languages (EN, FR, ES)  
- Make expense tracking easy even for non-tech users  

⚙️ **Installation and Execution**
1. Clone the repository  
2. Make sure you have Python 3 installed  
3. Install dependencies: `pip install -r requirements.txt`  
4. Run the app: `python project.py`

📦 **File Structure**
- `project.py` — main application logic  
- `utils.py` — helper functions (e.g. saving, formatting, file ops)  
- `messages.py` — multilingual interface strings  
- `test_project.py` — automated tests for all functions  
- `expenses.json` — stored expense data  
- `budget_limits.json` — user-defined budget limits  
- `README.md` — this file :)  
- `requirements.txt` — required packages  

💡 **Usage Example**
- Manually enter your expenses  
- Review category totals and budget warnings  
- Use filters to analyze spending by date  
- Get monthly summaries and smart tips  

🌐 **Multilingual Support**
We use a `messages` dictionary for English 🇬🇧, French 🇫🇷, and Spanish 🇪🇸.  
You can switch the language at startup.

✅ **Test Coverage**
All major features are covered by `pytest`:
- Adding and calculating expenses  
- Filtering by date  
- Budget checking  
- Multilingual output  
- File operations  

🎥 **Demo Video**
👉 https://youtu.be/Az6s3Clpmto

📌 **Author**
Created by **Yuri Oshurko** 🇨🇦, as part of the CS50P final project.
Location: Montreal, Canada.
📛 GitHub Username: Yuri-ai-std
🎓 edX Username: yuri_447
📅 Video recorded on: July 30, 2025

