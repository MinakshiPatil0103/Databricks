from flask import jsonify, request
from database.connection import execute_query
from flask import Blueprint

inventory_routes = Blueprint('inventory', __name__)

@inventory_routes.route("/unique_product_categories", methods=["GET"])
def unique_product_categories():
    """
    This api will get all unique product categories from the database
    """
    try:
        query = "SELECT DISTINCT product_category FROM tbl_available_stock_anom"
        result = execute_query(query)

        if result:
            unique_categories = [row[0] for row in result]
            return jsonify(unique_categories), 200
        else:
            return jsonify({"status": 404, "message": "No product categories found"}), 404

    except Exception as e:
        error_message = f"Error retrieving product categories: {str(e)}"
        return jsonify({"status": 500, "message": error_message}), 500

@inventory_routes.route("/unique_warehouse_locations", methods=["GET"])
def unique_warehouse_locations():
    """
    This api will get all unique warehouse locations from the database
    """
    try:
        query = "SELECT DISTINCT warehouse_location FROM tbl_available_stock_anom"
        result = execute_query(query)

        if result:
            unique_locations = [row[0] for row in result]
            return jsonify(unique_locations), 200
        else:
            return jsonify({"status": 404, "message": "No warehouse locations found"}), 404

    except Exception as e:
        error_message = f"Error retrieving warehouse locations: {str(e)}"
        return jsonify({"status": 500, "message": error_message}), 500

@inventory_routes.route("/stocked_out_products_all_locations", methods=["GET"])
def stocked_out_products_all_locations():
    """
    Retrieve stocked out products data across all warehouse locations.

    This endpoint fetches information about products that are currently stocked out due to less supply,
    providing a comprehensive view of stockouts across all warehouse locations.
    ---
    tags:
    - Current stocked out products, Stocked out products across all locations
    parameters:
    - None required

    Example query:
    1. /stocked_out_products_all_locations

    Response includes:
    - Warehouse Location
    - Total stocked out products
    - List of Product Codes affected by stockout
    """
    try:
        query = """
            SELECT 
                warehouse_location,
                COUNT(*) as total_stocked_out_products,
                STUFF((
                    SELECT ', ' + product_code
                    FROM tbl_oos_anom t2
                    WHERE t2.warehouse_location = t1.warehouse_location
                    AND t2.stock_out_reason = 'stockout due to less supply'
                    FOR XML PATH('')
                ), 1, 2, '') as stockout_product_codes
            FROM tbl_oos_anom t1
            WHERE stock_out_reason = 'stockout due to less supply'
            GROUP BY warehouse_location
            ORDER BY total_stocked_out_products DESC
        """
        result = execute_query(query)

        if result:
            stocked_out_data_by_locations = []
            for row in result:
                product_codes = row[2].split(",") if row[2] else []
                product_codes = [code.strip() for code in product_codes if code.strip()]

                location_data = {
                    "Warehouse location": row[0],
                    "Total stocked out products": row[1],
                    "Product codes": product_codes,
                }
                stocked_out_data_by_locations.append(location_data)

            return jsonify(stocked_out_data_by_locations), 200
        else:
            return jsonify({"status": 404, "message": "No stocked out products found"}), 404

    except Exception as e:
        error_message = f"Error retrieving stocked out products: {str(e)}"
        return jsonify({"status": 500, "message": error_message}), 500

@inventory_routes.route("/overstocked_products_by_location_and_product", methods=["GET"])
def overstocked_products_by_location_and_product():
    """
    Retrieve top overstocked products across all warehouse locations.
    
    This endpoint returns the top 10 products classified as 'excess', including:
      - Product Code: Identifier of the product.
      - Warehouse Location: Where the product is stored.
      - Cover Days: The number of days the current stock covers.
      - Current Stock: The current stock at hand.
      - Ideal Stock: The ideal required stock.
      - Stock Difference: The difference between current and ideal stock.
    
    The results are ordered by stock difference in descending order.
    """
    try:
        query = """
            SELECT TOP 10 
                product_code,
                warehouse_location,
                cover_days,
                stock_at_hand,
                ideal_required_quantity,
                stock_diff
            FROM tbl_available_stock_anom 
            WHERE stock_status = 'excess'
            ORDER BY stock_diff DESC
        """
        result = execute_query(query)

        if result:
            overstocked_items = [
                {
                    "Product Code": row[0],
                    "Warehouse Location": row[1],
                    "Cover Days": row[2],
                    "Current Stock": row[3],
                    "Ideal Stock": row[4],
                    "Stock Difference": row[5],
                }
                for row in result
            ]
            return jsonify(overstocked_items), 200
        else:
            return jsonify({"status": 404, "message": "No overstocked items found."}), 404

    except Exception as e:
        error_message = f"Error retrieving overstocked items: {str(e)}"
        return jsonify({"status": 500, "message": error_message}), 500

@inventory_routes.route("/understocked_products_by_location_and_product", methods=["GET"])
def understocked_products_by_location_and_product():
    """
    Retrieve top understocked products across all warehouse locations.
    
    This endpoint returns the top 10 products classified as 'shortage', including:
      - Product Code: Identifier of the product.
      - Warehouse Location: Where the product is stored.
      - Cover Days: The number of days the current stock covers.
      - Current Stock: The current stock at hand.
      - Ideal Stock: The ideal required stock.
      - Stock Difference: The difference between current and ideal stock.
    
    The results are ordered by stock difference in ascending order.
    """
    try:
        query = """
            SELECT TOP 10
                product_code, 
                warehouse_location,
                cover_days,
                stock_at_hand,
                ideal_required_quantity,
                stock_diff
            FROM tbl_available_stock_anom 
            WHERE stock_status = 'shortage'
            ORDER BY stock_diff ASC
        """
        result = execute_query(query)

        if result:
            understocked_items = [
                {
                    "Product Code": row[0],
                    "Warehouse Location": row[1],
                    "Cover Days": row[2],
                    "Current Stock": row[3],
                    "Ideal Stock": row[4],
                    "Stock Difference": row[5],
                }
                for row in result
            ]
            return jsonify(understocked_items), 200
        else:
            return jsonify({"message": "No understocked items found"}), 404

    except Exception as e:
        return jsonify({
            "error": f"Failed to retrieve understocked products: {str(e)}",
            "status": "error"
        }), 500

@inventory_routes.route("/stock_distribution_and_status_across_all_locations", methods=["GET"])
def stock_distribution_and_status_across_all_locations():
    """
    Retrieve comprehensive stock level summary across all warehouse locations.

    This endpoint provides a detailed breakdown of stock status distribution for each warehouse,
    including counts of partially stocked, fully stocked, and Stocked out products.
    ---
    tags: Stock Distribution and Status, Stock Levels Summary
    parameters: None required
    Example query: /stock_distribution_and_status_across_all_locations
    Response includes:
    - Warehouse Location
    - Partially Stocked Count
    - Fully Stocked Count
    - Stocked Out Count
    - Total Items Count
    """
    try:
        stock_levels_query = """
            WITH available_stock_summary AS (
                SELECT 
                    warehouse_location,
                    SUM(CASE WHEN stock_status = 'shortage' THEN 1 ELSE 0 END) as partially_stocked,
                    SUM(CASE WHEN stock_status = 'excess' THEN 1 ELSE 0 END) as fully_stocked
                FROM 
                    tbl_available_stock_anom
                GROUP BY 
                    warehouse_location
            ),
            oos_summary AS (
                SELECT 
                    warehouse_location,
                    COUNT(*) as completely_stockout
                FROM 
                    tbl_oos_anom
                GROUP BY 
                    warehouse_location
            )
            SELECT 
                a.warehouse_location,
                a.partially_stocked,
                a.fully_stocked,
                ISNULL(o.completely_stockout, 0) as completely_stockout
            FROM 
                available_stock_summary a
            LEFT JOIN 
                oos_summary o ON a.warehouse_location = o.warehouse_location
            ORDER BY 
                a.warehouse_location
        """
        results = execute_query(stock_levels_query)

        if not results:
            return jsonify({
                "message": "No stock level data found",
                "status": "empty",
                "data": []
            }), 404

        stock_levels_data = []
        total_partially_stocked = 0
        total_fully_stocked = 0
        total_completely_stockout = 0

        for row in results:
            warehouse = str(row[0]) if row[0] else "Unknown Location"
            partially_stocked = max(0, int(row[1])) if row[1] is not None else 0
            fully_stocked = max(0, int(row[2])) if row[2] is not None else 0
            completely_stockout = max(0, int(row[3])) if row[3] is not None else 0

            total_partially_stocked += partially_stocked
            total_fully_stocked += fully_stocked
            total_completely_stockout += completely_stockout

            total_items = partially_stocked + fully_stocked + completely_stockout
            stock_levels_data.append({
                "Warehouse Location": warehouse,
                "Partially Stocked": partially_stocked,
                "Fully Stocked": fully_stocked,
                "Stocked Out": completely_stockout,
                "Total Items": total_items,
            })

        return jsonify(stock_levels_data), 200

    except Exception as e:
        error_message = f"Error retrieving stock levels: {str(e)}"
        return jsonify({"error": error_message, "status": "error", "data": None}), 500

@inventory_routes.route("/inventory_variance_analysis_across_locations", methods=["GET"])
def inventory_variance_analysis_across_locations():
    """
    Retrieve top 5 positive and negative stock variances across warehouse locations.

    This endpoint analyzes stock deviations from ideal levels, providing insights into both
    excess and shortage scenarios across the warehouse network.
    ---
    tags:
    - Inventory Analytics
    - Stock Deviation Metrics
    - Excess Analysis
    - Shortage Analysis
    - Variance Percentage
    - Location Performance
    """
    try:
        # Query for top 5 positive variances
        positive_query = """
            SELECT TOP 5
                warehouse_location, 
                product_code, 
                product_category,
                stock_diff,
                stock_at_hand,
                ideal_required_quantity
            FROM tbl_available_stock_anom 
            WHERE stock_diff > 0
            ORDER BY stock_diff DESC
        """
        
        # Query for top 5 negative variances
        negative_query = """
            SELECT TOP 5
                warehouse_location, 
                product_code, 
                product_category,
                stock_diff,
                stock_at_hand,
                ideal_required_quantity
            FROM tbl_available_stock_anom 
            WHERE stock_diff < 0
            ORDER BY stock_diff ASC
        """

        positive_results = execute_query(positive_query)
        negative_results = execute_query(negative_query)

        def process_variance_data(results):
            variance_data = []
            for row in results:
                current_stock = row[4]
                ideal_stock = row[5]
                variance_percentage = (
                    ((current_stock - ideal_stock) / ideal_stock * 100)
                    if ideal_stock > 0
                    else 0
                )
                item = {
                    "Warehouse Location": row[0],
                    "Product Code": row[1],
                    "Product Category": row[2],
                    "Stock Difference": row[3],
                    "Current Stock": current_stock,
                    "Ideal Stock Requirement": ideal_stock,
                    "Variancy": f"{round(variance_percentage, 1)}%",
                }
                variance_data.append(item)
            return variance_data

        response = {
            "positive_variance_data": process_variance_data(positive_results),
            "negative_variance_data": process_variance_data(negative_results)
        }
        return jsonify(response), 200

    except Exception as e:
        error_message = f"Error processing stock variance data: {str(e)}"
        return jsonify({"status": 500, "message": error_message}), 500

@inventory_routes.route("/expected_stock_requirements_for_products_till_month_ends", methods=["GET"])
def expected_stock_requirements_for_products_till_month_ends():
    """
    Retrieve top 10 products by total required stock and warehouse distribution.

    This endpoint provides insights into product demand patterns by analyzing required stock levels
    and their distribution across warehouse locations.
    ---
    tags: Required Stocks Till Month Ends, Expected Stock Levels, Product Demand Forecast Analysis
    """
    try:
        query = """
            SELECT TOP 10
                product_code,
                SUM(required_stock) as total_required_stock,
                COUNT(DISTINCT warehouse_location) as location_count
            FROM 
                tbl_available_stock_anom
            GROUP BY 
                product_code
            ORDER BY 
                total_required_stock DESC
        """
        result = execute_query(query)

        if not result:
            return jsonify({"status": 404, "message": "No stock level data found"}), 404

        expected_stock_levels = [
            {
                "Product Code": row[0],
                "Required Stock": int(row[1]),
                "Warehouse Location Count": int(row[2]),
            }
            for row in result
        ]

        return jsonify(expected_stock_levels), 200

    except Exception as e:
        error_message = f"Error retrieving expected stock levels: {str(e)}"
        return jsonify({"status": 500, "message": error_message}), 500

@inventory_routes.route("/cover_days_summary", methods=["GET"])
def cover_days_summary():
    """
    Retrieve product coverage distribution across predefined time ranges.

    This endpoint analyzes inventory coverage periods, grouping products by their cover days
    into meaningful time ranges (0-5, 6-10, 11-30, 31-60, over 60 days) across warehouse locations.
    ---
    tags:
    - Inventory Coverage
    - Time Range Analysis
    - Stock Duration
    - Distribution Metrics
    """
    try:
        query = """
            SELECT 
                CASE 
                    WHEN cover_days BETWEEN 0 AND 5 THEN '0-5 days'
                    WHEN cover_days BETWEEN 6 AND 10 THEN '6-10 days'
                    WHEN cover_days BETWEEN 11 AND 30 THEN '11-30 days'
                    WHEN cover_days BETWEEN 31 AND 60 THEN '31-60 days'
                    WHEN cover_days > 60 THEN 'Over 60 days'
                END as cover_days_range,
                COUNT(*) as product_count
            FROM 
                tbl_available_stock_anom
            GROUP BY 
                CASE 
                    WHEN cover_days BETWEEN 0 AND 5 THEN '0-5 days'
                    WHEN cover_days BETWEEN 6 AND 10 THEN '6-10 days'
                    WHEN cover_days BETWEEN 11 AND 30 THEN '11-30 days'
                    WHEN cover_days BETWEEN 31 AND 60 THEN '31-60 days'
                    WHEN cover_days > 60 THEN 'Over 60 days'
                END
            ORDER BY 
                MIN(cover_days)
        """
        results = execute_query(query)

        if not results:
            return jsonify({"status": 404, "message": "No cover days distribution data found"}), 404

        distribution_data = [
            {
                "Cover Days Range": row[0],
                "Product Code and Location pair count": row[1],
            }
            for row in results
        ]

        return jsonify(distribution_data), 200

    except Exception as e:
        error_message = f"Error retrieving cover days distribution: {str(e)}"
        return jsonify({"status": 500, "message": error_message}), 500

@inventory_routes.route("/discontinued_products_across_all_warehouse_locations", methods=["GET"])
def discontinued_products_across_all_warehouse_locations():
    """
    Retrieve discontinued products data across all warehouse locations.

    This endpoint fetches information about discontinued products, including counts and product codes,
    for each warehouse location. Results are sorted by discontinued product count in descending order.
    ---
    tags:
    - Discontinued Products for all Locations
    """
    try:
        query = """
            SELECT 
                warehouse_location,
                COUNT(DISTINCT product_code) as discontinued_products_count,
                STUFF((
                    SELECT ', ' + product_code
                    FROM tbl_oos_anom t2
                    WHERE t2.warehouse_location = t1.warehouse_location
                    AND t2.stock_out_reason = 'stockout due to discontinuation'
                    FOR XML PATH('')
                ), 1, 2, '') as discontinued_product_codes
            FROM 
                tbl_oos_anom t1
            WHERE 
                stock_out_reason = 'stockout due to discontinuation'
            GROUP BY 
                warehouse_location
            ORDER BY 
                discontinued_products_count DESC
        """
        results = execute_query(query)

        if results:
            discontinued_products = []
            for row in results:
                product_codes = row[2].split(",") if row[2] else []
                product_codes = [code.strip() for code in product_codes if code.strip()]
                discontinued_data = {
                    "Warehouse Location": row[0],
                    "Discontinued Product Count": row[1],
                    "Discontinued Product Codes": product_codes,
                }
                discontinued_products.append(discontinued_data)

            return jsonify(discontinued_products), 200
        else:
            return jsonify({"status": 404, "message": "No discontinued products data found"}), 404

    except Exception as e:
        error_message = f"Error retrieving discontinued products data: {str(e)}"
        return jsonify({"status": 500, "message": error_message}), 500

@inventory_routes.route("/get_inventory_level_for_products_and_locations", methods=["GET"])
def get_inventory_level_for_products_and_locations():
    """
    Retrieve Available stock at hand or Inventory levels for all products based on optional filters.
    
    This endpoint fetches available stock at hand for products in different warehouse locations.
    Users can filter results by specifying either a `location`, a `product_code`, or both.
    ---
    tags:
    - Inventory Levels or Stock at hand for Products and Locations
    """
    try:
        location = request.args.get("location", "").strip()
        product_code = request.args.get("product_code", "").strip()
        
        if not location and not product_code:
            return jsonify({"status": 400, "message": "At least one parameter (location or product_code) is required"}), 400
        
        conditions = []
        if location:
            conditions.append(f"warehouse_location = '{location}'")
        if product_code:
            conditions.append(f"product_code = '{product_code}'")
            
        where_clause = " AND ".join(conditions)
        
        query = f"""
            SELECT 
                warehouse_location,
                product_code,
                stock_at_hand
            FROM tbl_available_stock_anom
            WHERE {where_clause}
            ORDER BY warehouse_location, stock_at_hand DESC
        """
        
        results = execute_query(query)

        if results:
            inventory_data = [
                {
                    "Warehouse Location": row[0],
                    "Product Code": row[1],
                    "Available stock at hand": row[2],
                }
                for row in results
            ]
            return jsonify(inventory_data), 200
        else:
            search_criteria = []
            if location:
                search_criteria.append(f"location: {location}")
            if product_code:
                search_criteria.append(f"product code: {product_code}")
                
            return jsonify({
                "status": 404,
                "message": f"No inventory data found for {' and '.join(search_criteria)}"
            }), 404

    except Exception as e:
        error_message = f"An error occurred while retrieving inventory data: {str(e)}"
        return jsonify({"status": 500, "message": error_message}), 500


@inventory_routes.route("/estimated_stockout_of_products_by_cover_days", methods=["GET"])
def estimated_stockout_of_products_by_cover_days():
    """
    Retrieve stockout estimates for warehouse locations based on cover days threshold.

    This endpoint fetches estimated stockout data for products across warehouse locations 
    based on a specified cover days threshold value.
    ---
    tags:
    - Estimated stocked out products, Stock estimates by X cover days, Risk of Stockout within X days
    """
    cover_days = request.args.get("cover_days", "").strip()
    if not cover_days:
        return jsonify({"status": 400, "message": "Cover days parameter is required"}), 400

    try:
        cover_days = int(cover_days)
    except ValueError:
        return jsonify({"status": 400, "message": "Invalid cover_days. Must be a valid number."}), 400

    if cover_days <= 0:
        return jsonify({"status": 400, "message": "Invalid cover_days. Must be a positive integer."}), 400

    try:
        query = f"""
            SELECT 
                warehouse_location,
                COUNT(*) as total_products,
                SUM(CASE WHEN cover_days <= {cover_days} THEN 1 ELSE 0 END) as products_below_threshold
            FROM tbl_available_stock_anom
            GROUP BY warehouse_location
            HAVING SUM(CASE WHEN cover_days <= {cover_days} THEN 1 ELSE 0 END) > 0
            ORDER BY products_below_threshold DESC
        """
        
        result = execute_query(query)

        if result:
            stock_estimates = [
                {
                    "Warehouse Location": row[0],
                    "Total Products": row[1],
                    "Count of Products under given days": row[2],
                }
                for row in result
            ]
            return jsonify(stock_estimates), 200
        else:
            return jsonify({
                "status": 404,
                "message": "No estimates found for the given criteria"
            }), 404

    except Exception as e:
        error_message = f"An error occurred while retrieving stock estimates: {str(e)}"
        return jsonify({"status": 500, "message": error_message}), 500
