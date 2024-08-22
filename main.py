from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import logging
import asyncio
import pyotp
import mysql.connector
from mysql.connector import Error
from SmartApi import SmartConnect
from datetime import datetime
from typing import List, Dict, Any
import json
# Setup logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
file_handler = logging.FileHandler('api_logs.log')
file_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

app = FastAPI()

# Define request model
class OrderData(BaseModel):
    instrument: str
    lot_quantity_buffer: int
    transactionType: str
    exchange: str
    orderType: str
    productType: str

class ExecuteOrdersRequest(BaseModel):
    teacher_id: int
    order_data: List[OrderData]

class ExitPendingRequest(BaseModel):
    teacher_id: int

def get_current_time_formatted():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

async def create_connection():
    try:
        connection = mysql.connector.connect(
            host='172.105.61.104',
            user='root',
            password='MahitNahi@12',
            database='stocksync',
            auth_plugin='mysql_native_password'
        )
        if connection.is_connected():
            return connection
    except Error as e:
        logger.error(f'{get_current_time_formatted()} - Error: {e}')
        return None

async def fetch_users(teacher_id: int):
    connection = await create_connection()
    if not connection:
        raise HTTPException(status_code=500, detail="Database connection failed")
    try:
        cursor = connection.cursor(dictionary=True)
        query = """
        SELECT * FROM user WHERE (user_id = %s OR teacher_id = %s)
          AND broker_conn_status = 1
          AND is_active = 1
          AND trade_status = 1
        """
        cursor.execute(query, (teacher_id, teacher_id))
        result = cursor.fetchall()
        return result
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

async def fetch_broker_credentials(user_id: int):
    connection = await create_connection()
    if not connection:
        logger.error(f"{get_current_time_formatted()} - Database connection failed")
        return None
    try:
        cursor = connection.cursor(dictionary=True)
        query = """
        SELECT client_id, password, qr_totp_token, api_key FROM admin_dashboard_broker_angleone WHERE user_id_id = %s
        """
        cursor.execute(query, (user_id,))
        result = cursor.fetchone()
        return result
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

async def process_orders_for_user(user: Dict[str, Any], instrument_data: List[Dict[str, Any]]):
    instrument_list = []

    for order in instrument_data:
        instrument = order.instrument
        lot_quantity_buffer = order.lot_quantity_buffer
        transactionType = order.transactionType
        exchange = order.exchange
        orderType = order.orderType
        productType = order.productType

        if not all([instrument, lot_quantity_buffer, transactionType, exchange, orderType, productType]):
            logger.error(f"{get_current_time_formatted()} - Missing order data for user {user['name']}")
            continue
        
        connection = await create_connection()
        if not connection:
            logger.error(f"{get_current_time_formatted()} - Database connection failed")
            continue

        try:
            cursor = connection.cursor(dictionary=True)
            cursor.execute("SELECT * FROM instrument WHERE symbol = %s", (instrument,))
            data2 = cursor.fetchone()

            if data2 is None:
                logger.error(f"{get_current_time_formatted()} - Token not found for instrument {instrument} for user {user['user_id']}")
                continue

            buyToken = data2['token']
            lotsize = data2['lotsize']
            order_data = {
                "buySymbol": instrument,
                "lotsize": lotsize,
                "buyToken": buyToken,
                "exchange": exchange,
                "ordertype": orderType,
                "producttype": productType,
                "lot_quantity_buffer": lot_quantity_buffer,
                "transactionType": transactionType
            }

            logger.info(f"{get_current_time_formatted()} - Order data of {instrument} for user {user['name']}")

            success = await broker_buy_order(user, order_data)
            if success:
                instrument_list.append(instrument)
            await asyncio.sleep(1)
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()

    return instrument_list

async def broker_buy_order(user: Dict[str, Any], order_data: Dict[str, Any]):
    broker_credentials = await fetch_broker_credentials(user['user_id'])
    if not broker_credentials:
        logger.error(f"{get_current_time_formatted()} - Broker credentials not found for user {user['user_id']}")
        return False

    broker_client_id = broker_credentials['client_id']
    broker_password = broker_credentials['password']
    broker_qr_totp_token = broker_credentials['qr_totp_token']
    api_key = broker_credentials['api_key']

    lot_size_limit = int(user['lot_size_limit'])                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    
    tradingsymbol = order_data["buySymbol"]
    symboltoken = order_data["buyToken"]
    lotsize = order_data["lotsize"]
    exchange = order_data["exchange"]
    ordertype = order_data["ordertype"]
    producttype = order_data["producttype"]
    lot_quantity_buffer = order_data["lot_quantity_buffer"]
    transactionType = order_data["transactionType"]
    
    try:
        token = broker_qr_totp_token
        totp = pyotp.TOTP(token).now()
    except Exception as e:
        logger.error(f"{get_current_time_formatted()} - Invalid TOTP: {e}")
        return False

    try:
        smartApi = SmartConnect(api_key)
        username = broker_client_id
        pwd = broker_password
        
        data = await asyncio.get_event_loop().run_in_executor(None, smartApi.generateSession, username, pwd, totp)
        
        if data["message"] != 'SUCCESS':
            logger.error(f"{get_current_time_formatted()} - Session generation failed for user {user['user_id']}")
            return False

        ltp_data = await asyncio.get_event_loop().run_in_executor(None, smartApi.ltpData, exchange, tradingsymbol, symboltoken)
        if 'data' not in ltp_data or 'ltp' not in ltp_data['data']:
            logger.error(f"{get_current_time_formatted()} - Failed to get LTP data for {tradingsymbol}")
            return False
        
        ltp = float(ltp_data['data']['ltp'])
        actual_quantity2 = int(lotsize * lot_size_limit)
        
        if actual_quantity2 <= 0:
            logger.error(f"{get_current_time_formatted()} - Actual quantity for {tradingsymbol} is less than or equal to 0")
            return False

        order_params = {
            "variety": "NORMAL",
            "tradingsymbol": tradingsymbol,
            "symboltoken": symboltoken,
            "transactiontype": transactionType,
            "exchange": exchange,
            "ordertype": ordertype,
            "producttype": producttype,
            "duration": "DAY",
            "price": str(ltp),
            "squareoff": "0",
            "stoploss": "0",
            "quantity": str(actual_quantity2)
        }

        response = await asyncio.get_event_loop().run_in_executor(None, smartApi.placeOrder, order_params)
        if not response or 'orderid' not in response:
            #logger.error(f"{get_current_time_formatted()} - Failed to place order for {tradingsymbol}: {response}")
            return False
        
        buy_order_id = response.get("orderid")

        if buy_order_id and len(buy_order_id) > 10 and buy_order_id.isdigit():
            connection = await create_connection()
            if not connection:
                logger.error(f"{get_current_time_formatted()} - Database connection failed")
                return False
            try:
                cursor = connection.cursor(dictionary=True)
                cursor.execute("""
                SELECT * FROM trade_book_live WHERE user_id = %s AND stock_symbol = %s AND stock_token = %s AND transactiontype = 'BUY'
                """, (user['user_id'], tradingsymbol, symboltoken))
                trade_queryset = cursor.fetchall()

                if trade_queryset:
                    total_lots = sum(item['lot_size'] for item in trade_queryset)
                    total_stock_quantity = sum(item['stock_quantity'] for item in trade_queryset)
                    avg_price = sum(item['price'] for item in trade_queryset) / len(trade_queryset)

                    total_stock_quantity += actual_quantity2
                    total_lots += lot_size_limit
                    avg_price = round(((avg_price * (total_lots - lot_size_limit)) + (ltp * lot_size_limit)) / total_lots, 2)

                    cursor.execute("""
                    UPDATE trade_book_live
                    SET stock_quantity = %s, lot_size = %s, price = %s, orderid = %s
                    WHERE user_id = %s AND stock_symbol = %s AND stock_token = %s AND transactiontype = 'BUY'
                    """, (total_stock_quantity, total_lots, avg_price, buy_order_id, user['user_id'], tradingsymbol, symboltoken))
                else:
                    cursor.execute("""
                    INSERT INTO trade_book_live
                    (user_id, stock_symbol, stock_token, stock_quantity, lot_size, price, orderid, transactiontype)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (user['user_id'], tradingsymbol, symboltoken, actual_quantity2, lot_size_limit, ltp, buy_order_id, 'BUY'))
                connection.commit()
                logger.info(f"{get_current_time_formatted()} - Order placed successfully for {tradingsymbol}")
                return True
            finally:
                if connection.is_connected():
                    cursor.close()
                    connection.close()
        else:
            logger.error(f"{get_current_time_formatted()} - Invalid order ID for {tradingsymbol}")
            return False
    except Exception as e:
        logger.error(f"{get_current_time_formatted()} - Error placing exit order: {e}")
        return False
    finally:
        smartApi.terminateSession(username)


@app.post("/execute_orders")
async def execute_orders_api(request: ExecuteOrdersRequest):
    try:
        users = await fetch_users(request.teacher_id)
        if not users:
            raise HTTPException(status_code=404, detail="No active users found")

        for user in users:
            if user.get('broker') != "angle_one":
                logger.info(f"{get_current_time_formatted()} - Skipping user {user['name']} as their broker is not 'angle_one'")
                continue
            
            instrument_list = await process_orders_for_user(user, request.order_data)
            logger.info(f"{get_current_time_formatted()} - Orders placed for user {user['name']} with instruments: {instrument_list}")

        return {"st": 1, "message": "Orders placed successfully"}
    except Exception as e:
        logger.error(f"{get_current_time_formatted()} - Error in placing orders: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")



async def process_student_pending_orders(user: Dict[str, Any]):
    logger.info(f"{get_current_time_formatted()} - Starting process_student_pending_orders for user {user['user_id']}")
    connection = await create_connection()
    if not connection:
        logger.error(f"{get_current_time_formatted()} - Database connection failed for user {user['user_id']}")
        return {"st": 3, "msg": f"Database connection failed for user {user['user_id']}"}

    try:
        cursor = connection.cursor(dictionary=True)
        query = """
        SELECT stock_symbol, stock_token, stock_quantity FROM trade_book_live
        WHERE user_id = %s AND orderid IS NOT NULL
        """
        cursor.execute(query, (user['user_id'],))
        trades_list = cursor.fetchall()
        logger.info(f"{get_current_time_formatted()} - Fetched trades list for user {user['user_id']}: {trades_list}")
    except Exception as e:
        logger.error(f"{get_current_time_formatted()} - Error executing query: {e}")
        return {"st": 3, "msg": f"Error fetching trades for user {user['user_id']}"}
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            logger.info(f"{get_current_time_formatted()} - Database connection closed for user {user['user_id']}")

    if trades_list:
        try:
            success = await broker_sell_students_all_pending_order(user, trades_list)
            if success:
                return {"st": 1, "msg": f"Exit order placed successfully for user {user['user_id']}"}
            else:
                return {"st": 3, "msg": f"Failed to place exit order for user {user['user_id']}"}
        except Exception as e:
            logger.error(f"{get_current_time_formatted()} - Error placing exit order for user {user['user_id']}: {e}")
            return {"st": 3, "msg": f"Failed to place exit order for user {user['user_id']}"}
    else:
        logger.info(f"{get_current_time_formatted()} - No trades found for user {user['user_id']}")
        return {"st": 2, "msg": f"No trades found for user {user['user_id']}"}

async def broker_sell_students_all_pending_order(user: Dict[str, Any], trades: List[Dict[str, Any]]):
    logger.info(f"{get_current_time_formatted()} - Starting broker_sell_students_all_pending_order for user {user['user_id']}")
    broker_credentials = await fetch_broker_credentials(user['user_id'])
    if not broker_credentials:
        logger.error(f"{get_current_time_formatted()} - Broker credentials not found for user {user['user_id']}")
        return False

    broker_client_id = broker_credentials['client_id']
    broker_password = broker_credentials['password']
    broker_qr_totp_token = broker_credentials['qr_totp_token']
    api_key = broker_credentials['api_key']
    logger.info(f"{get_current_time_formatted()} - Broker credentials fetched for user {user['user_id']}")

    try:
        totp = pyotp.TOTP(broker_qr_totp_token).now()
        logger.info(f"{get_current_time_formatted()} - Generated TOTP for user {user['user_id']}")
    except Exception as e:
        logger.error(f"{get_current_time_formatted()} - Invalid TOTP: {e}")
        return False

    try:
        smartApi = SmartConnect(api_key)
        session_data = await asyncio.get_event_loop().run_in_executor(None, smartApi.generateSession, broker_client_id, broker_password, totp)
        logger.info(f"{get_current_time_formatted()} - Session data for user {user['user_id']}: {session_data}")

        if session_data.get("message") != 'SUCCESS':
            logger.error(f"{get_current_time_formatted()} - Session generation failed for user {user['user_id']}: {session_data}")
            return False

        try:
            for trade in trades:
                tradingsymbol = trade["stock_symbol"]
                symboltoken = trade["stock_token"]
                try:
                    quantity = int(float(trade["stock_quantity"]))  # Convert to integer if needed
                except ValueError as e:
                    logger.error(f"{get_current_time_formatted()} - Invalid quantity format for trade {trade}: {e}")
                    continue

                order_params = {
                    "variety": "NORMAL",
                    "tradingsymbol": tradingsymbol,
                    "symboltoken": symboltoken,
                    "transactiontype": "SELL",
                    "exchange": "NFO",
                    "ordertype": "MARKET",
                    "producttype": "MIS",
                    "duration": "DAY",
                    "quantity": quantity
                }
                logger.info(f"{get_current_time_formatted()} - Placing order with params: {order_params}")

                try:
                    response = await asyncio.get_event_loop().run_in_executor(None, smartApi.placeOrder, order_params)
                    
                    # Log raw response for debugging
                    logger.info(f"{get_current_time_formatted()} - Raw response from placeOrder: {response}")

                    if not response:
                        logger.error(f"{get_current_time_formatted()} - Empty response from placeOrder for {tradingsymbol}")
                        continue

                    if isinstance(response, str):
                        try:
                            response_data = json.loads(response)
                            logger.info(f"{get_current_time_formatted()} - Parsed response: {response_data}")
                        except json.JSONDecodeError as e:
                            logger.error(f"{get_current_time_formatted()} - Couldn't parse the JSON response received from the server: {response}")
                            logger.error(f"{get_current_time_formatted()} - Error: {e}")
                            continue
                    else:
                        response_data = response

                    logger.info(f"{get_current_time_formatted()} - Response from placeOrder: {response_data}")

                    if response_data and 'status' in response_data and response_data['status'] == 'SUCCESS':
                        logger.info(f"{get_current_time_formatted()} - Exit order placed for {tradingsymbol} with order ID {response_data['data']['orderId']}")
                    else:
                        logger.error(f"{get_current_time_formatted()} - Failed to place exit order for {tradingsymbol}. Response: {response_data}")
                        logger.error(f"{get_current_time_formatted()} - Error code: {response_data.get('errorCode', 'Unknown')}")
                        logger.error(f"{get_current_time_formatted()} - Error message: {response_data.get('errorMessage', 'Unknown')}")
                        continue

                except Exception as e:
                    logger.error(f"{get_current_time_formatted()} - Error placing exit order for {tradingsymbol}: {e}")
                    continue

                await asyncio.sleep(1)  # Rate limit handling

        except Exception as e:
            logger.error(f"{get_current_time_formatted()} - Error placing exit orders: {e}")
            return False

    finally:
        try:
            await asyncio.get_event_loop().run_in_executor(None, smartApi.terminateSession, broker_client_id)
            logger.info(f"{get_current_time_formatted()} - Session terminated for user {user['user_id']}")
        except Exception as e:
            logger.error(f"{get_current_time_formatted()} - Error terminating session for user {user['user_id']}: {e}")

    return True

@app.post("/exit_all_student_pending")
async def exit_all_student_pending(request: Dict[str, Any]):
    logger.info(f"{get_current_time_formatted()} - Received request to exit_all_student_pending: {request}")
    try:
        teacher_id = request.get('teacher_id')

        if not teacher_id:
            logger.error(f"{get_current_time_formatted()} - Teacher ID is required")
            raise HTTPException(status_code=400, detail="Teacher ID is required")

        users = await fetch_users(teacher_id)
        if not users:
            logger.error(f"{get_current_time_formatted()} - No active users found for teacher_id {teacher_id}")
            raise HTTPException(status_code=404, detail="No active users found")

        logger.info(f"{get_current_time_formatted()} - Found {len(users)} active users")
        tasks = [process_student_pending_orders(user) for user in users]
        results = await asyncio.gather(*tasks)
        logger.info(f"{get_current_time_formatted()} - Exit orders processed for all users")

        summary = {
            "st": 1,
            "msg": "Success",
            "total_users": len(users),
            "results": results
        }
        return summary

    except Exception as e:
        logger.error(f"{get_current_time_formatted()} - Error in exit_all_student_pending: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
