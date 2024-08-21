# # Model
# class ExitOrderData(BaseModel):
#     instrument: str
#     lot_quantity_buffer: int
#     transactionType: str
#     exchange: str
#     orderType: str
#     productType: str

# class ExitPositionRequest(BaseModel):
#     teacher_id: int
#     order_data: List[ExitOrderData]

# # API Endpoints
# @app.post("/exit_position")
# async def exit_position_api(request: ExitPositionRequest):
#     try:
#         users = await fetch_users(request.teacher_id)
#         if not users:
#             raise HTTPException(status_code=404, detail="No active users found")

#         for user in users:
#             if user.get('broker') != "angle_one":
#                 logger.info(f"{get_current_time_formatted()} - Skipping user {user['name']} as their broker is not 'angle_one'")
#                 continue
            
#             instrument_list = await process_exit_orders_for_user(user, request.order_data)
#             logger.info(f"{get_current_time_formatted()} - Exit orders placed for user {user['name']} with instruments: {instrument_list}")

#         return {"status": "success", "message": "Exit orders placed successfully"}
#     except Exception as e:
#         logger.error(f"{get_current_time_formatted()} - Error in placing exit orders: {e}")
#         raise HTTPException(status_code=500, detail="Internal server error")

# async def process_exit_orders_for_user(user: Dict[str, Any], exit_order_data: List[Dict[str, Any]]):
#     instrument_list = []

#     for order in exit_order_data:
#         instrument = order.instrument
#         lot_quantity_buffer = order.lot_quantity_buffer
#         transactionType = order.transactionType
#         exchange = order.exchange
#         orderType = order.orderType
#         productType = order.productType

#         if not all([instrument, lot_quantity_buffer, transactionType, exchange, orderType, productType]):
#             logger.error(f"{get_current_time_formatted()} - Missing exit order data for user {user['name']}")
#             continue
        
#         connection = await create_connection()
#         if not connection:
#             logger.error(f"{get_current_time_formatted()} - Database connection failed")
#             continue

#         try:
#             cursor = connection.cursor(dictionary=True)
#             cursor.execute("SELECT * FROM instrument WHERE symbol = %s", (instrument,))
#             data2 = cursor.fetchone()

#             if data2 is None:
#                 logger.error(f"{get_current_time_formatted()} - Token not found for instrument {instrument} for user {user['user_id']}")
#                 continue

#             sellToken = data2['token']
#             lotsize = data2['lotsize']
#             order_data = {
#                 "sellSymbol": instrument,
#                 "lotsize": lotsize,
#                 "sellToken": sellToken,
#                 "exchange": exchange,
#                 "ordertype": orderType,
#                 "producttype": productType,
#                 "lot_quantity_buffer": lot_quantity_buffer,
#                 "transactionType": transactionType
#             }

#             logger.info(f"{get_current_time_formatted()} - Exit order data of {instrument} for user {user['name']}")

#             success = await broker_sell_order(user, order_data)
#             if success:
#                 instrument_list.append(instrument)
#             await asyncio.sleep(1)
#         finally:
#             if connection.is_connected():
#                 cursor.close()
#                 connection.close()

#     return instrument_list

# async def broker_sell_order(user: Dict[str, Any], order_data: Dict[str, Any]):
#     broker_credentials = await fetch_broker_credentials(user['user_id'])
#     if not broker_credentials:
#         logger.error(f"{get_current_time_formatted()} - Broker credentials not found for user {user['user_id']}")
#         return False

#     broker_client_id = broker_credentials['client_id']
#     broker_password = broker_credentials['password']
#     broker_qr_totp_token = broker_credentials['qr_totp_token']
#     api_key = broker_credentials['api_key']

#     lot_size_limit = int(user['lot_size_limit'])
#     tradingsymbol = order_data["sellSymbol"]
#     symboltoken = order_data["sellToken"]
#     lotsize = order_data["lotsize"]
#     exchange = order_data["exchange"]
#     ordertype = order_data["ordertype"]
#     producttype = order_data["producttype"]
#     lot_quantity_buffer = order_data["lot_quantity_buffer"]
#     transactionType = order_data["transactionType"]

#     try:
#         token = broker_qr_totp_token
#         totp = pyotp.TOTP(token).now()
#     except Exception as e:
#         logger.error(f"{get_current_time_formatted()} - Invalid TOTP: {e}")
#         return False

#     try:
#         smartApi = SmartConnect(api_key)
#         username = broker_client_id
#         pwd = broker_password

#         data = await asyncio.get_event_loop().run_in_executor(None, smartApi.generateSession, username, pwd, totp)
        
#         if data["message"] != 'SUCCESS':
#             logger.error(f"{get_current_time_formatted()} - Session generation failed for user {user['user_id']}")
#             return False

#         ltp_data = await asyncio.get_event_loop().run_in_executor(None, smartApi.ltpData, exchange, tradingsymbol, symboltoken)
#         if 'data' not in ltp_data or 'ltp' not in ltp_data['data']:
#             logger.error(f"{get_current_time_formatted()} - Failed to get LTP data for {tradingsymbol}")
#             return False
        
#         ltp = float(ltp_data['data']['ltp'])
#         actual_quantity2 = int(lotsize * lot_size_limit)
        
#         if actual_quantity2 <= 0:
#             logger.error(f"{get_current_time_formatted()} - Actual quantity for {tradingsymbol} is less than or equal to 0")
#             return False

#         order_params = {
#             "variety": "NORMAL",
#             "tradingsymbol": tradingsymbol,
#             "symboltoken": symboltoken,
#             "transactiontype": transactionType,
#             "exchange": exchange,
#             "ordertype": ordertype,
#             "producttype": producttype,
#             "duration": "DAY",
#             "price": str(ltp),
#             "squareoff": "0",
#             "stoploss": "0",
#             "quantity": str(actual_quantity2)
#         }

#         response = await asyncio.get_event_loop().run_in_executor(None, smartApi.placeOrder, order_params)
#         if not response or 'orderid' not in response:
#             logger.error(f"{get_current_time_formatted()} - Failed to place sell order for {tradingsymbol}")
#             return False
        
#         sell_order_id = response.get("orderid")

#         if sell_order_id and len(sell_order_id) > 10 and sell_order_id.isdigit():
#             connection = await create_connection()
#             if not connection:
#                 logger.error(f"{get_current_time_formatted()} - Database connection failed")
#                 return False
#             try:
#                 cursor = connection.cursor(dictionary=True)
#                 cursor.execute("""
#                 SELECT * FROM trade_book_live WHERE user_id = %s AND stock_symbol = %s AND stock_token = %s AND transactiontype = 'BUY'
#                 """, (user['user_id'], tradingsymbol, symboltoken))
#                 trade_queryset = cursor.fetchall()

#                 if trade_queryset:
#                     total_lots = sum(item['lot_size'] for item in trade_queryset)
#                     total_stock_quantity = sum(item['stock_quantity'] for item in trade_queryset)
#                     avg_price = sum(item['price'] for item in trade_queryset) / len(trade_queryset)

#                     total_stock_quantity -= actual_quantity2
#                     total_lots -= lot_size_limit
#                     avg_price = round(((avg_price * (total_lots + lot_size_limit)) - (ltp * lot_size_limit)) / total_lots, 2)

#                     cursor.execute("""
#                     UPDATE trade_book_live
#                     SET stock_quantity = %s, lot_size = %s, price = %s, order_id = %s
#                     WHERE user_id = %s AND stock_symbol = %s AND stock_token = %s AND transactiontype = 'BUY'
#                     """, (total_stock_quantity, total_lots, avg_price, sell_order_id, user['user_id'], tradingsymbol, symboltoken))
#                 else:
#                     cursor.execute("""
#                     INSERT INTO trade_book_live
#                     (user_id, stock_symbol, stock_token, stock_quantity, lot_size, price, order_id, transactiontype)
#                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
#                     """, (user['user_id'], tradingsymbol, symboltoken, -actual_quantity2, -lot_size_limit, ltp, sell_order_id, 'SELL'))
#                 connection.commit()
#                 logger.info(f"{get_current_time_formatted()} - Exit order placed successfully for {tradingsymbol}")
#                 return True
#             finally:
#                 if connection.is_connected():
#                     cursor.close()
#                     connection.close()
#         else:
#             logger.error(f"{get_current_time_formatted()} - Invalid order ID received for {tradingsymbol}")
#             return False
#     except Exception as e:
#         logger.error(f"{get_current_time_formatted()} - Error placing sell order: {e}")
#         return False

