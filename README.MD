# Python FastAPI Project

This project is a Python-based web application backend built using FastAPI. It demonstrates a modern API backend with dependencies such as MongoDB, Pydantic, and Uvicorn.

## Features

- FastAPI for building APIs
- MongoDB integration via PyMongo
- Data validation with Pydantic
- Asynchronous server with Uvicorn

## Requirements

- Python 3.8 or higher
- All dependencies listed in `requirement.txt`

## Setup Instructions

1. **Clone the repository or copy the project files** to your local system.
   ```sh
   git clone https://github.com/bishal292/task-app-backend.git
   ```

2. **Navigate to the project directory:**
   ```sh
   cd task-app-backend
   ```

3. **(Optional) Create and activate a virtual environment:**
   ```sh
   python -m venv venv
   # On Windows:
   venv\Scripts\activate
   # On Unix or MacOS:
   source venv/bin/activate
   ```

4. **Install dependencies:**
   ```sh
   pip install -r requirement.txt
   ```

5. **Run the FastAPI application:**
   ```sh
   uvicorn app.main:app --reload
   ```
   Replace `main:app` with the actual filename and FastAPI app instance if different(Changed), As for this project FastAPI instance is located inside "app/main.py" and instance is named as 'app'.

6. **Access the API:**
   - Open your browser and go to: [http://127.0.0.1:8000](http://127.0.0.1:8000)
   - API documentation is available at: [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)  ||  [http://127.0.0.1:8000/redoc](http://127.0.0.1:8000/redoc)

## Notes

- Ensure MongoDB is running if your project uses it.
- Update configuration files as needed for your environment.

## License

This project is for educational purposes.

