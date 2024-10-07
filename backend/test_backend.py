import asyncio
import aiohttp
import json

BASE_URL = "http://localhost:8000"  # Adjust this if your server is running on a different host/port


async def test_endpoint(session, endpoint, name):
    url = f"{BASE_URL}{endpoint}"
    async with session.get(url) as response:
        if response.status == 200:
            print(f"{name} endpoint test: SUCCESS")
            print("Response content:")
            content = await response.json()
            print(json.dumps(content, indent=2))
        else:
            print(f"{name} endpoint test: FAILED (Status code: {response.status})")
            print("Response content:")
            print(await response.text())
        print("-" * 50)


async def run_all_tests():
    endpoints = [
        ("/", "Root"),
        ("/kpis", "KPIs"),
        ("/customer_segments", "Customer Segments"),
        ("/monthly_revenue", "Monthly Revenue"),
        ("/top_customers", "Top Customers"),
        ("/product_category_performance", "Product Category Performance"),
        ("/customer_satisfaction", "Customer Satisfaction"),
        ("/churn_risk", "Churn Risk"),
        ("/rfm_segmentation", "RFM Segmentation"),
    ]

    async with aiohttp.ClientSession() as session:
        tasks = [test_endpoint(session, endpoint, name) for endpoint, name in endpoints]
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(run_all_tests())
