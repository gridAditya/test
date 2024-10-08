from fastapi import FastAPI
from fastapi.responses import JSONResponse
import backend_logic

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Welcome to the P&G CDP Dashboard API"}


@app.get("/kpis")
async def api_get_kpis():
    return await backend_logic.get_kpis()


@app.get("/customer_segments")
async def api_get_customer_segments():
    return JSONResponse(content=await backend_logic.get_customer_segments())


@app.get("/monthly_revenue")
async def api_get_monthly_revenue():
    return JSONResponse(content=await backend_logic.get_monthly_revenue())


@app.get("/top_customers")
async def api_get_top_customers():
    return await backend_logic.get_top_customers()


@app.get("/product_category_performance")
async def api_get_product_category_performance():
    return JSONResponse(content=await backend_logic.get_product_category_performance())


@app.get("/customer_satisfaction")
async def api_get_customer_satisfaction():
    return JSONResponse(content=await backend_logic.get_customer_satisfaction())


@app.get("/churn_risk")
async def api_get_churn_risk():
    return JSONResponse(content=await backend_logic.get_churn_risk())


@app.get("/rfm_segmentation")
async def api_get_rfm_segmentation():
    return JSONResponse(content=await backend_logic.get_rfm_segmentation())


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
	
