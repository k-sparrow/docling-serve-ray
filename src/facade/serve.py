import uvicorn

if __name__ == "__main__":
    uvicorn.run("facade.main:app", host="0.0.0.0", port=8000)
