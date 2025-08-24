from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from bson import ObjectId
from pymongo import MongoClient
# Exporting producer for sending message to rabbit MQ.
from app.producer import publish_message

# MongoDB connection
client = MongoClient("mongodb://localhost:27017/") # Mongo DB UrI String for connection
db = client["task_demo_db"]  # Database Name
collection = db["tasks"]    # Collection Name -> tasks

app = FastAPI()     # Creating FastApI object for backend as like as Express in node.
@app.get("/")
def read_root():
    '''
    Root Route for checking either it is running or not
    '''
    return {"message": "Hello, FastAPI is running!"}

# Pydantic model -> Task
class Task(BaseModel):
    title: str
    description: str
    completed: bool = False


def task_serializer(task) -> dict:
    '''
    Utility to convert MongoDB ObjectId to string and each task object as a Dictionary.
    :param task:task object from MongoDB
    :return: Dictionary
    '''
    return {
        "id": str(task["_id"]),
        "title": task["title"],
        "description": task["description"],
        "completed": task["completed"],
    }


# Create Task
@app.post("/tasks")
def create_task(task: Task):
    '''
    Create a new task
    :param task:
    :return: Created Task Object
    '''
    if not task.title or not task.description:
        raise HTTPException(status_code=400, detail="missing required fields title and description")
    new_task = dict(task)
    publish_message("task_queue", {"action":"create", "task":new_task})
    result = collection.insert_one(new_task)
    # Publishing message to RabbitMQ queue.
    created_task = collection.find_one({"_id": result.inserted_id})
    return task_serializer(created_task)


# Get all tasks
@app.get("/tasks")
def get_tasks():
    '''
    Retrieve all tasks
    :return: List of Task Objects
    '''
    tasks = collection.find()
    return [task_serializer(task) for task in tasks]


# Get single task by ID
@app.get("/tasks/{task_id}")
def get_task(task_id: str):
    '''
    Retrieve a task by its ID
    :param task_id: string
    :return:
    '''
    if not ObjectId.is_valid(task_id):
        raise HTTPException(status_code=404, detail="invalid task id")
    task = collection.find_one({"_id": ObjectId(task_id)})
    publish_message("task_queue", {"action": "read", "task ID": task_id})
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task_serializer(task)


# Update Task ( expect an object of Task)
@app.put("/tasks/{task_id}")
def update_task(task_id: str, updated_task: Task):
    '''
    Update a task by its ID
    :param task_id:
    :param updated_task:
    :return:
    '''
    if not ObjectId.is_valid(task_id):
        raise HTTPException(status_code=404, detail="invalid task id")
    publish_message("task_queue", {"action": "update", "task ID": task_id})
    result = collection.update_one(
        {"_id": ObjectId(task_id)}, {"$set": dict(updated_task)}
    )
    if result.modified_count == 0:
        raise HTTPException(status_code=404, detail="Task not found or no changes Made.")
    task = collection.find_one({"_id": ObjectId(task_id)})
    return task_serializer(task)


# Delete Task
@app.delete("/tasks/{task_id}")
def delete_task(task_id: str):
    '''
    Delete a task with matching ID
    :param task_id:
    :return:
    '''
    if not ObjectId.is_valid(task_id):
        raise HTTPException(status_code=404, detail="invalid task id")
    result = collection.delete_one({"_id": ObjectId(task_id)})
    publish_message("task_queue", {"action": "delete", "task ID": task_id})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Task not found")
    return {"message": "Task deleted successfully"}
