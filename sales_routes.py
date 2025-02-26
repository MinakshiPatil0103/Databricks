from flask import jsonify, request
from database.connection import execute_query
from flask import Blueprint

sales_routes = Blueprint('sales', __name__)

@sales_routes.route("/unique_product_categories", methods=["GET"])
def unique_product_categories():
    """
    This api will get all unique product categories from the database
    """
    try:
        query = "SELECT DISTINCT product_category FROM tbl_sales_forecast"
        result = execute_query(query)

        if result:
            unique_categories = [row[0] for row in result]
            return jsonify(unique_categories), 200
        else:
            return jsonify({"status": 404, "message": "No product categories found"}), 404

    except Exception as e:
        error_message = f"Error retrieving product categories: {str(e)}"
        return jsonify({"status": 500, "message": error_message}), 500

@sales_routes.route("/unique_warehouse_locations", methods=["GET"])
def unique_warehouse_locations():
    """
    This api will get all unique warehouse locations from the database
    """
    try:
        query = "SELECT DISTINCT warehouse_location FROM tbl_sales_forecast"
        result = execute_query(query)

        if result:
            unique_locations = [row[0] for row in result]
            return jsonify(unique_locations), 200
        else:
            return jsonify({"status": 404, "message": "No warehouse locations found"}), 404

    except Exception as e:
        error_message = f"Error retrieving warehouse locations: {str(e)}"
        return jsonify({"status": 500, "message": error_message}), 500

@sales_routes.route("/top_products_and_their_projected_demand", methods=["GET"])
def top_products_and_their_projected_demand():
    """
    Retrieve the top 10 products along with their projected daily demand.
    
    This endpoint calculates the average current sales rate, current forecast rate, and next month's forecast rate per day
    for the top products, ordered by the next month's forecast rate in descending order.
    """
    try:
        query = """
            SELECT TOP 10
                Product_Code,
                AVG(MTDSalesRate) As CurrentSalesRatePerDay,
                AVG(M1ForecastRate) As CurrentForecastRatePerDay,
                AVG(M2Forecast/26) AS NextMonthsForecastRatePerDay
            FROM 
                tbl_sales_forecast
            GROUP BY 
                Product_Code
            ORDER BY 
                NextMonthsForecastRatePerDay DESC
        """
        results = execute_query(query)

        response = [
            {
                "Product Code": row[0],
                "Current Month Sales Rate": abs(int(float(row[1]))) if row[1] else 0.0,
                "Current Month Forecast Rate": abs(int(float(row[2]))) if row[2] else 0.0,
                "Next Month Forecast Rate": abs(int(float(row[3]))) if row[3] else 0.0,
            }
            for row in results
        ]

        return jsonify(response), 200

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@sales_routes.route("/overselling_products_based_on_sales", methods=["GET"])
def overselling_products_based_on_sales():
    """
    Retrieve aggregated sales and forecast metrics for products marked as overselling.
    
    This endpoint returns data for each overselling product, including:
      - Product Code: Identifier of the product.
      - Total Forecast per day: Average forecast rate.
      - Total Sales per day: Average sales rate.
      - Sales Difference per day: Absolute difference between sales and forecast.
      
    The results are ordered by the sales difference in ascending order.
    """
    try:
        query = """
            SELECT 
                Product_Code,
                AVG(MTDSalesRate) AS TotalSalesPerDay,
                AVG(M1ForecastRate) AS TotalForecastRatePerDay,
                AVG(ForecastError) AS SalesDifference
            FROM tbl_sales_forecast
            WHERE Selling_Status = 'Overselling'
            GROUP BY Product_Code
            ORDER BY SalesDifference ASC
        """
        results = execute_query(query)

        response = [
            {
                "Product Code": row[0],
                "Total Forecast per day": abs(int(float(row[2]))) if row[2] else 0.0,
                "Total Sales per day": abs(int(float(row[1]))) if row[1] else 0.0,
                "Oversold per day": abs(int(float(row[3]))) if row[3] else 0.0,
            }
            for row in results
        ]

        return jsonify(
            {
                "message": "All overselling products across all locations.",
                "data": response,
            }
        ), 200

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@sales_routes.route("/underselling_products_based_on_sales", methods=["GET"])
def underselling_products_based_on_sales():
    """
    Retrieve aggregated sales and forecast metrics for products marked as underselling.
    
    This endpoint returns data for each underselling product, including:
      - Product Code: Identifier of the product.
      - Total Forecast per day: Average forecast rate.
      - Total Sales per day: Average sales rate.
      - Sales Difference per day: Absolute difference between sales and forecast.
      
    The results are ordered by the total sales per day in descending order.
    """
    try:
        query = """
            SELECT 
                Product_Code,
                AVG(MTDSalesRate) AS TotalSalesPerDay,
                AVG(M1ForecastRate) AS TotalForecastRatePerDay,
                AVG(ForecastError) AS SalesDifference
            FROM tbl_sales_forecast
            WHERE Selling_Status = 'Underselling'
            GROUP BY Product_Code
            ORDER BY TotalSalesPerDay DESC
        """
        results = execute_query(query)

        response = [
            {
                "Product Code": row[0],
                "Total Forecast per day": abs(int(float(row[2]))) if row[2] else 0.0,
                "Total Sales per day": abs(int(float(row[1]))) if row[1] else 0.0,
                "Undersold per day": abs(int(float(row[3]))) if row[3] else 0.0,
            }
            for row in results
        ]

        return jsonify(
            {
                "message": "All underselling products across all locations.",
                "data": response,
            }
        ), 200

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@sales_routes.route("/sales_rate_by_product_and_location", methods=["GET"])
def sales_rate_by_product_and_location():
    """
    Retrieve the daily sales rate for a given product across all warehouse locations.
    
    This endpoint accepts a GET request with a query parameter 'product_code'.
    Example query: /sales_rate_by_product_and_location?product_code=123
    
    If the product_code parameter is missing or invalid, an appropriate error message is returned.
    """
    product_code = request.args.get("product_code", "").strip()
    if not product_code:
        return jsonify({"status": "error", "message": "Product code is required"}), 400

    try:
        product_code = int(product_code)
    except ValueError:
        return jsonify({"status": "error", "message": "Product code must be an integer"}), 400

    try:
        query = f"""
            SELECT 
                warehouse_location,
                AVG(MTDSalesRate) AS Sales
            FROM
                tbl_sales_forecast
            WHERE
                Product_Code = {product_code}
            GROUP BY
                warehouse_location
            ORDER BY
                Sales DESC
        """
      
        results = execute_query(query)

        response = [
            {
                "Warehouse Location": row[0],
                "Sales rate per day": abs(int(row[1])) if row[1] else 0.0,
            }
            for row in results
        ]

        return jsonify(response), 200

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@sales_routes.route("/forecast_sales_vs_actual_sales_for_products_and_locations", methods=["GET"])
def forecast_sales_vs_actual_sales_for_products_and_locations():
    """
    Retrieve aggregated forecast and actual sales metrics for a given product across all warehouse locations.
    
    This endpoint accepts a GET request with a query parameter 'product_code'.
    Example query: /forecast_sales_vs_actual_sales_for_products_and_locations?product_code=123
    
    If the product_code parameter is missing or invalid, an appropriate error message is returned.
    """
    product_code = request.args.get("product_code", "").strip()
    if not product_code:
        return jsonify({"status": "error", "message": "Product code is required"}), 400

    try:
        product_code = int(product_code)
    except ValueError:
        return jsonify({"status": "error", "message": "Product code must be an integer"}), 400

    try:
        query = f"""
          SELECT
                warehouse_location,
                product_code,
                AVG(M1ForecastRate) AS Total_Forecast,
                AVG(MTDSalesRate) AS Total_Actual_Sales,
                AVG(ForecastError) AS Total_Forecast_Error
            FROM
                tbl_sales_forecast
            WHERE   
                product_code = {product_code}
            GROUP BY
                warehouse_location, product_code
            ORDER BY
                Total_Forecast DESC
        """
        results = execute_query(query)

        response = [
            {
                "Warehouse Location": row[0],
                "Product Code": row[1],
                "Average Forecast Rate": abs(int(float(row[2]))) if row[2] else 0.0,
                "Average Sales Rate": abs(int(float(row[3]))) if row[3] else 0.0,
                "Average Forecast Error": abs(int(float(row[4]))) if row[4] else 0.0,
            }
            for row in results
        ]
        return jsonify(response), 200
        
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

