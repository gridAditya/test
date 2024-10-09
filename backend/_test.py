import pytest

from colorama import Fore, Style
from dateutil.parser import isoparse
from backend_logic import get_db_session, get_kpis, get_customer_segments, get_monthly_revenue, get_top_customers, get_product_category_performance, get_customer_satisfaction, get_churn_risk, get_rfm_segmentation

def is_valid_datetime(date_string):
    try:
        isoparse(date_string)
        return True
    except ValueError:
        return False

@pytest.mark.asyncio
async def test_kpis():
    result = None
    ## Test the API Response
    try:
        result = await get_kpis()
        print(f"{Fore.GREEN}{Style.BRIGHT}[+] get_kpis() endpoint is responding...{Style.RESET_ALL}")
    except Exception as e:
        print(f"{Fore.RED}{Style.BRIGHT}[-] Unable to get response from API: {e}{Style.RESET_ALL}")
        return
    
    ## Validate the API output
    try:
        assert isinstance(result, dict), "[-] API not returning output in desired format"
        print(f"{Fore.BLUE}{Style.BRIGHT}[+] API returned output in desired format...{Style.RESET_ALL}")
        required_keys = {"total_customers", "total_lifetime_value", "average_order_value", "retention_rate"}
        try:
            assert required_keys.issubset(result.keys()), f"Missing keys: {required_keys - result.keys()}"
            print(f"{Fore.BLUE}{Style.BRIGHT}[+] API output contains all the required properties.{Style.RESET_ALL}")
            try:
                required_keys = [("total_customers", int), ("total_lifetime_value", float), ("average_order_value", float), ("retention_rate", float)]
                for key, key_type in required_keys:
                    assert isinstance(result[key], key_type), f"[-] Invalid data-type for {key}, expected data-type: {key_type}, returned data-type: {type(result[key])}"
                    print(f"{Fore.BLUE}{Style.BRIGHT}[+] Valid data-type for {key}: {key_type}.{Style.RESET_ALL}")
            except AssertionError as e:
                print(f"{Fore.RED}{Style.BRIGHT}{e}{Style.RESET_ALL}")
                return
        except AssertionError as e:
            print(f"{Fore.RED}{Style.BRIGHT}{e}{Style.RESET_ALL}")
            return
        print(f"{Fore.GREEN}{Style.BRIGHT}[+] All tests passed for get_kpis() endpoint{Style.RESET_ALL}")
    except AssertionError as e:
        print(f"{Fore.RED}{Style.BRIGHT}{e}{Style.RESET_ALL}")

@pytest.mark.asyncio
async def test_customer_segments():
    result = None
     ## Test the API Response
    try:
        result = await get_customer_segments()
        print(f"{Fore.GREEN}{Style.BRIGHT}[+] get_customer_segments() endpoint is responding...{Style.RESET_ALL}")
    except Exception as e:
        print(f"{Fore.RED}{Style.BRIGHT}[-] Unable to get response from API: {e}{Style.RESET_ALL}")
        return
    
    required_labels = ["Low Value", "High Value", "Medium Value"]
    required_type = int
   
    ## Validate the API output
    try:
        for label in result["data"][0]["labels"]:
            assert label in required_labels, f"[-] Invalid label detected: {label}"
        for idx, value in enumerate(result["data"][0]["values"]):
            assert isinstance(value, required_type), f"[-] Invalid data-type for label {result['data'][0]['labels'][idx]}: {type(value)}, expected data-type: int"
        
        print(f"{Fore.BLUE}{Style.BRIGHT}[+] API is returning output in desired format with valid data-types.{Style.RESET_ALL}")
        print(f"{Fore.GREEN}{Style.BRIGHT}[+] All tests passed for get_customer_segments() endpoint.{Style.RESET_ALL}")
    except AssertionError as e:
        print(f"{Fore.RED}{Style.BRIGHT}{e}{Style.RESET_ALL}")

@pytest.mark.asyncio
async def test_monthly_revenue():
    result = None
    try:
        result = await get_monthly_revenue()
        print(f"{Fore.GREEN}{Style.BRIGHT}[+] get_monthly_revenue() endpoint is responding...{Style.RESET_ALL}")
    except Exception as e:
        print(f"{Fore.RED}{Style.BRIGHT}[-] Unable to get response from API: {e}{Style.RESET_ALL}")
        return

    try:
        for value in result["data"][0]["x"]:
            assert is_valid_datetime(value), f"[-] Value {value} not a valid date."
        
        for value in result["data"][0]["y"]:
            assert isinstance(value, float), f"[-] Invalid data type for value: {value} detected. Expected: float Returned: {type(value)}"
        
        print(f"{Fore.GREEN}{Style.BRIGHT}[+] All tests for get_monthly_revenue() passed...{Style.RESET_ALL}")
    except AssertionError as e:
        print(f"{Fore.RED}{Style.BRIGHT}{e}{Style.RESET_ALL}")

@pytest.mark.asyncio
async def test_get_top_customers():
    result = None
    try:
        result = await get_top_customers()
        print(f"{Fore.GREEN}{Style.BRIGHT}[+] get_top_customers() endpoint is responding properly.{Style.RESET_ALL}")
    except Exception as e:
        print(f"{Fore.RED}{Style.BRIGHT}[-] get_top_customers() endpoint not responding...{Style.RESET_ALL}")
        return
    
    required_field_and_types = {"customer_id": int, "first_name": str, "last_name": str, "total_lifetime_value": float}

    try:
        for customer in result:
            for key, value in customer.items():
                assert (key in required_field_and_types) and (isinstance(value, required_field_and_types[key])), f"[-] Unexpected {key} with value-type: {type(value)}."
        print(f"{Fore.GREEN}{Style.BRIGHT}[+] All tests for get_top_customers() passed...{Style.RESET_ALL}")
    except AssertionError as e:
        print(f"{Fore.RED}{Style.BRIGHT}{e}{Style.RESET_ALL}")

@pytest.mark.asyncio
async def test_product_category_performance():
    result = None
    ## Test the responsiveness of the endpoint
    try:
        result = await get_product_category_performance()
        print(f"{Fore.GREEN}{Style.BRIGHT}[+] get_product_category_performance() endpoint is responding properly.{Style.RESET_ALL}")
    except Exception as e:
        print(f"{Fore.RED}{Style.BRIGHT}[-] get_product_category_performance() endpoint not responding...{Style.RESET_ALL}")
        return

    ## Validate the output from the endpoint
    try:
        for value in result["data"][0]["x"]:
            assert isinstance(value, str), f"[-] Unexpected data-type encountered: {type(value)}, Expected: String"
        
        for value in result["data"][0]["y"]:
            assert isinstance(value, float), f"[-] Unexpected data-type encountered: {type(value)}, Expected: float"
        
        print(f"{Fore.GREEN}{Style.BRIGHT}[+] All tests for get_product_category_performance() passed...{Style.RESET_ALL}")
    except AssertionError as e:
        print(f"{Fore.RED}{Style.BRIGHT}{e}{Style.RESET_ALL}")

@pytest.mark.asyncio
async def test_average_customer_satisfaction():
    result = None
    ## Test the responsiveness of the endpoint
    try:
        result = await get_customer_satisfaction()
        print(f"{Fore.GREEN}{Style.BRIGHT}[+] get_customer_satisfaction() endpoint is responding properly.{Style.RESET_ALL}")
    except Exception as e:
        print(f"{Fore.RED}{Style.BRIGHT}[-] get_customer_satisfaction() endpoint not responding...{Style.RESET_ALL}")
        return
    
    ## Validate the output from the endpoint
    try:
        value = result["data"][0]["gauge"]["threshold"]["value"]
        assert isinstance(value, float), f"[-] Unexpected Data-type: {type(value)}, Expected: float"
        print(f"{Fore.GREEN}{Style.BRIGHT}[+] All tests for get_customer_satisfaction() passed...{Style.RESET_ALL}")
    except AssertionError as e:
        print(f"{Fore.RED}{Style.BRIGHT}{e}{Style.RESET_ALL}")

@pytest.mark.asyncio
async def test_get_churn_risk():
    result = None
    ## Test the responsiveness of the endpoint
    try:
        result = await get_churn_risk()
        print(f"{Fore.GREEN}{Style.BRIGHT}[+] get_churn_risk() endpoint is responding properly.{Style.RESET_ALL}")
    except Exception as e:
        print(f"{Fore.RED}{Style.BRIGHT}[-] get_churn_risk() endpoint not responding...{Style.RESET_ALL}")
        return

    ## Validate the output from the endpoint
    try:
        for label in result["data"][0]["labels"]:
            assert isinstance(label, str), f"[-] Unexpected Data-type {type(label)}, Expected: String"
        
        for value in result["data"][0]["values"]:
            assert isinstance(value, int), f"[-] Unexpected Data-type {type(value)}, Expected: Int"

        print(f"{Fore.GREEN}{Style.BRIGHT}[+] All tests for get_churn_risk() passed...{Style.RESET_ALL}")
    except AssertionError as e:
        print(f"{Fore.RED}{Style.BRIGHT}{e}{Style.RESET_ALL}")

@pytest.mark.asyncio
async def test_get_rfm_segmentation():
    result = None
    ## Test the responsiveness of the endpoint
    try:
        result = await get_rfm_segmentation()
        print(f"{Fore.GREEN}{Style.BRIGHT}[+] get_rfm_segmentation() endpoint is responding properly.{Style.RESET_ALL}")
    except Exception as e:
        print(f"{Fore.RED}{Style.BRIGHT}[-] get_rfm_segmentation() endpoint not responding...{Style.RESET_ALL}")
        return

    ## Validate the output from the endpoint
    try:
        # print(result["data"][0])
        for value in result["data"][0]["x"]:
            assert isinstance(value, int), f"[-] Unexpected Data-Type {type(value)}, Expected: float"

        for value in result["data"][0]["y"]:
            assert isinstance(value, int), f"[-] Unexpected Data-Type {type(value)}, Expected: float"

        for value in result["data"][0]["z"]:
            assert isinstance(value, float), f"[-] Unexpected Data-Type {type(value)}, Expected: float"

        print(f"{Fore.GREEN}{Style.BRIGHT}[+] All tests for get_rfm_segmentation() passed...{Style.RESET_ALL}")
    except AssertionError as e:
        print(f"{Fore.RED}{Style.BRIGHT}{e}{Style.RESET_ALL}")
