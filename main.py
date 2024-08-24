import sys
import os

project_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_dir)

import pymysql
from pymysql.cursors import DictCursor

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import logging
import asyncio
import pyotp
from SmartApi import SmartConnect
from datetime import datetime
from typing import List, Dict, Any
import json
# import mysql.connector
# from mysql.connector import Error

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
        connection = pymysql.connect(
            host='172.105.61.104',
            user='root',
            password='MahitNahi@12',
            database='stocksync',
            cursorclass=DictCursor
        )
        return connection
    except pymysql.MySQLError as e:
        logger.error(f'{get_current_time_formatted()} - Error: {e}')
        return None

# Fetch users
async def fetch_users(teacher_id: int):
    connection = await create_connection()
    if not connection:
        raise HTTPException(status_code=500, detail="Database connection failed")
    try:
        with connection.cursor() as cursor:
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
        connection.close()

# Fetch broker credentials
async def fetch_broker_credentials(user_id: int):
    connection = await create_connection()
    if not connection:
        logger.error(f"{get_current_time_formatted()} - Database connection failed")
        return None
    try:
        with connection.cursor() as cursor:
            query = """
            SELECT client_id, password, qr_totp_token, api_key FROM admin_dashboard_broker_angleone WHERE user_id_id = %s
            """
            cursor.execute(query, (user_id,))
            result = cursor.fetchone()
            return result
    finally:
        connection.close()


# Process orders for a user
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
            with connection.cursor() as cursor:
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
            connection.close()

    return instrument_list

# Broker buy order function
async def insert_trade_book_live(connection, user_id, tradingsymbol, symboltoken, actual_quantity2, lot_size_limit, ltp, buy_order_id, exchange, ordertype, producttype, duration, transactionType):
    try:
        with connection.cursor() as cursor:
            currentDateAndTime = datetime.now()
            insert_query = """
            INSERT INTO trade_book_live
            (user_id, stock_symbol, stock_token, stock_quantity, lot_size, price, orderid, exchange, ordertype, producttype, duration, transactiontype, datetime)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            insert_values = (
                user_id,
                tradingsymbol,
                symboltoken,
                str(actual_quantity2),
                str(lot_size_limit),
                str(ltp),
                str(buy_order_id),
                str(exchange),
                str(ordertype),
                str(producttype),
                str(duration),
                str(transactionType),
                str(currentDateAndTime)
            )
            
            logger.info(f"{get_current_time_formatted()} - Executing INSERT query: {insert_query}")
            logger.info(f"{get_current_time_formatted()} - Query values: {insert_values}")
            
            cursor.execute(insert_query, insert_values)
            affected_rows = cursor.rowcount
            logger.info(f"{get_current_time_formatted()} - Affected rows: {affected_rows}")
            connection.commit()
            
            logger.info(f"{get_current_time_formatted()} - Trade book updated successfully for {tradingsymbol}")
            return True
    except pymysql.Error as e:
        logger.error(f"{get_current_time_formatted()} - MySQL Error: {e}")
        connection.rollback()
        return False
    except Exception as e:
        logger.error(f"{get_current_time_formatted()} - Unexpected error during database insertion: {str(e)}")
        connection.rollback()
        return False

async def broker_buy_order(user: Dict[str, Any], order_data: Dict[str, Any]):
    logger.info(f"{get_current_time_formatted()} - Starting broker_buy_order for user {user['user_id']}")
    
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
        duration="DAY"

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
            "duration": duration,
            "price": str(ltp),
            "squareoff": "0",
            "stoploss": "0",
            "quantity": str(actual_quantity2)
        }

        logger.info(f"{get_current_time_formatted()} - Placing order with params: {order_params}")
        try:
            response = await asyncio.get_event_loop().run_in_executor(None, smartApi.placeOrder, order_params)
            logger.info(f"{get_current_time_formatted()} - Order placement response: {response}")
            
            if not response:
                logger.error(f"{get_current_time_formatted()} - No response received from placeOrder for {tradingsymbol}")
                return False
            
            # Check if the response is a string (likely an order ID)
            if isinstance(response, str) and response.isdigit():
                buy_order_id = response
                logger.info(f"{get_current_time_formatted()} - Order placed successfully. Order ID: {buy_order_id}")
            elif isinstance(response, dict):
                # Handle dictionary response (as before)
                if 'status' in response:
                    if response['status'] != True:
                        logger.error(f"{get_current_time_formatted()} - Order placement failed for {tradingsymbol}. Status: {response['status']}, Message: {response.get('message', 'No message')}")
                        return False
                else:
                    logger.error(f"{get_current_time_formatted()} - Unexpected response format for {tradingsymbol}. Response: {response}")
                    return False
                
                if 'orderid' not in response:
                    logger.error(f"{get_current_time_formatted()} - OrderID not found in response for {tradingsymbol}. Response: {response}")
                    return False
                
                buy_order_id = response.get("orderid")
            else:
                logger.error(f"{get_current_time_formatted()} - Unexpected response type for {tradingsymbol}. Response: {response}")
                return False

            logger.info(f"{get_current_time_formatted()} - Order placed successfully. Order ID: {buy_order_id}")

            if buy_order_id and len(str(buy_order_id)) > 10 and str(buy_order_id).isdigit():
                logger.info(f"{get_current_time_formatted()} - Attempting to update trade book for order ID: {buy_order_id}")
                
                connection = await create_connection()
                if not connection:
                    logger.error(f"{get_current_time_formatted()} - Database connection failed")
                    return False
                
                try:
                    # Test database connection
                    with connection.cursor() as cursor:
                        cursor.execute("SELECT 1")
                        result = cursor.fetchone()
                        logger.info(f"Database connection test result: {result}")

                    insert_success = await insert_trade_book_live(
                        connection, 
                        user['user_id'], 
                        tradingsymbol, 
                        symboltoken,
                        actual_quantity2, 
                        lot_size_limit, 
                        ltp, 
                        buy_order_id,
                        exchange,
                        ordertype,
                        producttype,
                        duration, 
                        transactionType
                    )
                    
                    if insert_success:
                        logger.info(f"{get_current_time_formatted()} - Trade book entry created successfully for order ID: {buy_order_id}")
                        return True
                    else:
                        logger.error(f"{get_current_time_formatted()} - Failed to create trade book entry for order ID: {buy_order_id}")
                        return False
                finally:
                    connection.close()
            else:
                logger.error(f"{get_current_time_formatted()} - Invalid order ID for {tradingsymbol}")
                return False

        except Exception as e:
            logger.error(f"{get_current_time_formatted()} - Error placing order for {tradingsymbol}: {str(e)}")
            return False

    except Exception as e:
        logger.error(f"{get_current_time_formatted()} - Error in broker_buy_order: {str(e)}")
        return False
    finally:
        try:
            smartApi.terminateSession(username)
            logger.info(f"{get_current_time_formatted()} - Session terminated for user {username}")
        except Exception as e:
            logger.error(f"{get_current_time_formatted()} - Error terminating session: {str(e)}")

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
        with connection.cursor() as cursor:
            query = """
            SELECT stock_symbol, stock_token, stock_quantity, price, orderid, transactiontype, lot_size, exchange, ordertype, producttype, duration FROM trade_book_live
            WHERE user_id = %s AND orderid IS NOT NULL
            """
            cursor.execute(query, (user['user_id'],))
            trades_list = cursor.fetchall()
            logger.info(f"{get_current_time_formatted()} - Fetched trades list for user {user['user_id']}: {trades_list}")
    except Exception as e:
        logger.error(f"{get_current_time_formatted()} - Error executing query: {e}")
        return {"st": 3, "msg": f"Error fetching trades for user {user['user_id']}"}
    finally:
        connection.close()
        logger.info(f"{get_current_time_formatted()} - Database connection closed for user {user['user_id']}")

    if trades_list:
        success = await broker_sell_students_all_pending_order(user, trades_list)
        if success:
            return {"st": 1, "msg": f"Exit orders placed successfully for user {user['user_id']}"}
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

    try:
        totp = pyotp.TOTP(broker_qr_totp_token).now()
    except Exception as e:
        logger.error(f"{get_current_time_formatted()} - Invalid TOTP: {e}")
        return False

    smartApi = SmartConnect(api_key)
    
    try:
        session_data = await asyncio.get_event_loop().run_in_executor(None, smartApi.generateSession, broker_client_id, broker_password, totp)
        if session_data.get("message") != 'SUCCESS':
            logger.error(f"{get_current_time_formatted()} - Session generation failed for user {user['user_id']}: {session_data}")
            return False

        for trade in trades:
            tradingsymbol = trade["stock_symbol"]
            symboltoken = trade["stock_token"]
            transaction_type1=trade["transactiontype"]
            exchange=trade["exchange"]
            ordertype=trade["ordertype"]
            producttype=trade["producttype"]
            duration=trade["duration"]
            lot_size_limit=trade["lot_size"]
            orderid=trade["orderid"]
            price=trade["price"]
           
            try:
                quantity = abs(int(float(trade["stock_quantity"])))
            except ValueError as e:
                logger.error(f"{get_current_time_formatted()} - Invalid quantity format for trade {trade}: {e}")
                continue

            transactionType=None
            if transaction_type1 == "BUY":
                transactionType="SELL"
            else:
                transactionType="BUY"
                
            order_params = {
                "variety": "NORMAL",
                "tradingsymbol": tradingsymbol,
                "symboltoken": symboltoken,
                "transactiontype": transactionType,
                "exchange": exchange,
                "ordertype": ordertype,
                "producttype": producttype,
                "duration": duration,
                "quantity": str(quantity)
            }
            logger.info(f"{get_current_time_formatted()} - Placing order with params: {order_params}")

            try:
                response = await asyncio.get_event_loop().run_in_executor(None, smartApi.placeOrder, order_params)
                logger.info(f"{get_current_time_formatted()} - Order placement response: {response}")
                
                connection = await create_connection()
                actual_quantity2=str(quantity)
                lot_size_limit=lot_size_limit
                ltp=price
                buy_order_id=orderid
                exchange=exchange
                ordertype=ordertype
                producttype=producttype
                duration=duration
                
                if not connection:
                    logger.error(f"{get_current_time_formatted()} - Database connection failed")
                    return False
                
                try:
                    # Test database connection
                    with connection.cursor() as cursor:
                        cursor.execute("SELECT 1")
                        result = cursor.fetchone()
                        logger.info(f"Database connection test result: {result}")

                    insert_success = await insert_trade_book_live(
                        connection,
                        user['user_id'],
                        tradingsymbol,
                        symboltoken,
                        actual_quantity2,
                        lot_size_limit,
                        ltp,
                        buy_order_id,
                        exchange,
                        ordertype,
                        producttype,
                        duration,
                        transactionType
                    )
                finally:
                    connection.close()
            
                if not response:
                    logger.error(f"{get_current_time_formatted()} - No response received from placeOrder for {tradingsymbol}")
                    return False
        
                if response:
                    if isinstance(response, str):
                        try:
                            response_data = json.loads(response)
                        except json.JSONDecodeError:
                            logger.error(f"{get_current_time_formatted()} - Invalid JSON response: {response}")
                            continue
                    else:
                        response_data = response

                    if response_data and 'status' in response_data and response_data['status'] == 'SUCCESS':
                        logger.info(f"{get_current_time_formatted()} - Exit order placed for {tradingsymbol} with order ID {response_data['data']['orderId']}")
                    else:
                        logger.error(f"{get_current_time_formatted()} - Failed to place exit order for {tradingsymbol}. Response: {response_data}")
                else:
                    logger.error(f"{get_current_time_formatted()} - Empty response from placeOrder for {tradingsymbol}")

            except Exception as e:
                logger.error(f"{get_current_time_formatted()} - Error placing exit order for {tradingsymbol}: {e}")

            await asyncio.sleep(0.5)  # Rate limit handling

    except Exception as e:
        logger.error(f"{get_current_time_formatted()} - Error in broker_sell_students_all_pending_order: {e}")
    finally:
        try:
            await asyncio.get_event_loop().run_in_executor(None, smartApi.terminateSession, broker_client_id)
            logger.info(f"{get_current_time_formatted()} - Session terminated for user {user['user_id']}")
        except Exception as e:
            logger.error(f"{get_current_time_formatted()} - Error terminating session for user {user['user_id']}: {e}")

    return True  # Return True even if some orders failed, to continue processing other users

@app.post("/exit_all_student_pending")
async def exit_all_student_pending(request: ExitPendingRequest):
    logger.info(f"{get_current_time_formatted()} - Received request to exit_all_student_pending: {request}")
    try:
        teacher_id = request.teacher_id

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






