#required imports 
from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from typing import List, Dict, Any
import logging
from datetime import datetime
import asyncio
import pyotp
from SmartApi import SmartConnect
from sqlalchemy.orm import Session
import time
from fastapi.middleware.cors import CORSMiddleware

#loggersettings
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
file_handler = logging.FileHandler('api_logs.log')
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

#fastapi declaration
app = FastAPI()

#corsmiddleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://autoprofito.com"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

#Database connection settings
DATABASE_URL = "mysql+pymysql://root:MahitNahi%4012@172.105.61.104/stocksync"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

#pydantic models for fastapi module
class User(Base):
    __tablename__ = "user"
    user_id = Column(Integer, primary_key=True)
    name = Column(String(50))
    teacher_id = Column(Integer)
    broker_conn_status = Column(Integer)
    is_active = Column(Integer)
    trade_status = Column(Integer)
    lot_size_limit = Column(Integer)
    broker = Column(String(50))

class BrokerCredentials(Base):
    __tablename__ = "admin_dashboard_broker_angleone"
    broker_id = Column(Integer, primary_key=True)
    user_id_id = Column(Integer, ForeignKey('user.user_id'))
    client_id = Column(String(50))
    password = Column(String(50))
    qr_totp_token = Column(String(50))
    api_key = Column(String(50))
    user = relationship("User")


class TradeBookLive(Base):
    __tablename__ = "trade_book_live"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('user.user_id'))
    stock_symbol = Column(String(50))
    stock_token = Column(String(50))
    stock_quantity = Column(Float)
    price = Column(Float)
    orderid = Column(String(50))
    transactiontype = Column(String(10))
    exchange = Column(String(20))
    ordertype = Column(String(20))
    producttype = Column(String(20))
    duration = Column(String(20))
    datetime = Column(DateTime)
    uniqueorderid = Column(String(50))
    lot_size = Column(Integer)
    user = relationship("User")

class Instrument(Base):
    __tablename__ = "instrument"
    id = Column(Integer, primary_key=True)
    symbol = Column(String(50))
    token = Column(String(50))
    lotsize = Column(Integer)

Base.metadata.create_all(bind=engine)

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
    only_teacher_execute: bool

class ExitPendingRequest(BaseModel):
    teacher_id: int

class ExitStudentInstrumentRequest(BaseModel):
    student_id: int
    instrument_data: Dict[str, str]

class ExitStudentAllInstrumentsRequest(BaseModel):
    student_id: int

class ExitPositionRequest(BaseModel):
    teacher_id: str
    order_data: List[Dict[str, Any]]

# Helper functions
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

async def fetch_users(db, teacher_id: int):
    return db.query(User).filter(
        (User.user_id == teacher_id) | (User.teacher_id == teacher_id),
        User.broker_conn_status == 1,
        User.is_active == 1,
        User.trade_status == 1
    ).all()

async def fetch_broker_credentials(db, user_id: int):
    return db.query(BrokerCredentials).filter(BrokerCredentials.user_id_id == user_id).first()

async def fetch_instrument(db, symbol: str):
    return db.query(Instrument).filter(Instrument.symbol == symbol).first()


async def fetch_order_details(smartApi, order_id):
    try:
        order_book = await asyncio.get_event_loop().run_in_executor(
            None,
            smartApi.orderBook
        )
        for order in order_book.get('data', []):
            if order.get('orderid') == order_id:
                return order
        return None
    except Exception as e:
        logger.error(f"Error fetching order details: {str(e)}")
        return None

#execute_orders
async def place_order(user, order_data, db):
    broker_credentials = await fetch_broker_credentials(db, user.user_id)
    if not broker_credentials:
        logger.error(f"Broker credentials not found for user {user.user_id}")
        return False

    instrument = await fetch_instrument(db, order_data.instrument)
    if not instrument:
        logger.error(f"Instrument not found: {order_data.instrument}")
        return False

    obj = SmartConnect(api_key=broker_credentials.api_key)

    try:
        data = obj.generateSession(broker_credentials.client_id, broker_credentials.password, pyotp.TOTP(broker_credentials.qr_totp_token).now())
        if data["message"] != "SUCCESS":
            logger.error(f"Failed to generate session for user {user.user_id}")
            return False

        ltp_data = obj.ltpData(order_data.exchange, order_data.instrument, instrument.token)
        if 'data' not in ltp_data or 'ltp' not in ltp_data['data']:
            logger.error(f"Failed to get LTP data for {order_data.instrument}. LTP data: {ltp_data}")
            return False

        ltp = float(ltp_data['data']['ltp'])
        quantity = int(instrument.lotsize * user.lot_size_limit)

        order_params = {
            "variety": "NORMAL",
            "tradingsymbol": order_data.instrument,
            "symboltoken": instrument.token,
            "transactiontype": order_data.transactionType,
            "exchange": order_data.exchange,
            "ordertype": order_data.orderType,
            "producttype": order_data.productType,
            "duration": "DAY",
            "price": str(ltp),
            "squareoff": "0",
            "stoploss": "0",
            "quantity": str(quantity)
        }

        for attempt in range(5):  # Retry up to 5 times
            try:
                response = obj.placeOrder(order_params)
                logger.info(f"Order placement response: {response}")

                if isinstance(response, str):  # Response is a string
                    order_id = response

                    # Fetch the order details
                    order_details = await fetch_order_details(obj, order_id)
                    if order_details:
                        uniqueorderid = order_details.get('uniqueorderid')
                        order_status = order_details.get('status', 'unknown')  # Ensure default value
                        
                        if uniqueorderid:
                            logger.info(f"Retrieved uniqueorderid: {uniqueorderid}")
                        if order_status == 'rejected':
                            rejection_reason = order_details.get('text', 'No reason provided')
                            logger.warning(f"Order {order_id} was rejected. Reason: {rejection_reason}")
                            return False

                        try:
                            new_trade = TradeBookLive(
                                user_id=user.user_id,
                                stock_symbol=order_data.instrument,
                                stock_token=instrument.token,
                                stock_quantity=quantity,
                                price=ltp,
                                orderid=order_id,
                                transactiontype=order_data.transactionType,
                                exchange=order_data.exchange,
                                ordertype=order_data.orderType,
                                producttype=order_data.productType,
                                duration="DAY",
                                datetime=datetime.now(),
                                uniqueorderid=uniqueorderid,
                                lot_size=user.lot_size_limit
                            )
                            db.add(new_trade)
                            db.commit()
                            logger.info(f"Trade recorded successfully for order ID {order_id}")
                            return True
                        except Exception as e:
                            logger.error(f"Error saving trade details: {str(e)}")
                            return False
                    else:
                        logger.error(f"Order details not found for order ID {order_id}")
                        return False
                else:
                    logger.error(f"Unexpected response format: {response}")
                    return False
            except Exception as e:
                logger.error(f"Exception in place_order on attempt {attempt + 1}: {str(e)}")
                time.sleep(2 ** attempt)
        return False
    except Exception as e:
        logger.error(f"Exception in place_order: {str(e)}")
        return False
    finally:
        try:
            obj.terminateSession(broker_credentials.client_id)
        except Exception as e:
            logger.error(f"Error terminating session: {str(e)}")

@app.post("/execute_orders/")
async def execute_orders_api(request: ExecuteOrdersRequest, db: Session = Depends(get_db)):
    try:
        users = await fetch_users(db, request.teacher_id)
        if not users:
            raise HTTPException(status_code=404, detail="No active users found")

        results = []
        for user in users:
            if user.broker != "angle_one":
                logger.info(f"Skipping user {user.name} as their broker is not 'angle_one'")
                continue
            
            user_results = []
            for order in request.order_data:
                if request.only_teacher_execute and user.user_id != request.teacher_id:
                    logger.info(f"Skipping user {user.name} as only_teacher_execute is True and this is not the teacher")
                    continue
                
                success = await place_order(user, order, db)
                user_results.append({
                    "instrument": order.instrument,
                    "success": success
                })
            results.append({
                "user_id": user.user_id,
                "name": user.name,
                "orders": user_results
            })

        return {"st": 1, "results": results, "msg": "Order Placed Successfully"}
    except Exception as e:
        logger.error(f"Error in execute_orders_api: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


async def process_student_pending_orders(user, trades, db):
    broker_credentials = await fetch_broker_credentials(db, user.user_id)
    if not broker_credentials:
        return {"user_id": user.user_id, "status": "failed", "message": "Broker credentials not found"}

    obj = SmartConnect(api_key=broker_credentials.api_key)
    try:
        data = obj.generateSession(broker_credentials.client_id, broker_credentials.password, pyotp.TOTP(broker_credentials.qr_totp_token).now())
        if data["message"] != "SUCCESS":
            return {"user_id": user.user_id, "status": "failed", "message": "Failed to generate session"}

        results = []
        for trade in trades:
            exit_transaction_type = "SELL" if trade.transactiontype == "BUY" else "BUY"
            order_params = {
                "variety": "NORMAL",
                "tradingsymbol": trade.stock_symbol,
                "symboltoken": trade.stock_token,
                "transactiontype": exit_transaction_type,
                "exchange": trade.exchange,
                "ordertype": trade.ordertype,
                "producttype": trade.producttype,
                "duration": trade.duration,
                "price": str(trade.price),
                "quantity": str(abs(int(trade.stock_quantity)))
            }

            # Implementing retry logic with exponential backoff
            for attempt in range(3):  # Retry up to 3 times
                try:
                    response = obj.placeOrder(order_params)
                    logger.info(f"Order placement response: {response}")

                    if isinstance(response, str):  # Response is a string
                        order_id = response

                        # Fetch the order details
                        order_details = await fetch_order_details(obj, order_id)
                        if order_details:
                            uniqueorderid = order_details.get('uniqueorderid')
                            order_status = order_details.get('status', 'unknown')  # Ensure default value
                        
                            if uniqueorderid:
                                logger.info(f"Retrieved uniqueorderid: {uniqueorderid}")

                            if order_status == 'rejected':
                                rejection_reason = order_details.get('text', 'No reason provided')
                                logger.warning(f"Order {order_id} was rejected. Reason: {rejection_reason}")
                                results.append({"instrument": trade.stock_symbol, "status": "failed", "message": f"Order rejected: {rejection_reason}"})
                                break

                            try:
                                new_trade = TradeBookLive(
                                    user_id=user.user_id,
                                    stock_symbol=trade.stock_symbol,
                                    stock_token=trade.stock_token,
                                    stock_quantity=-trade.stock_quantity,
                                    price=trade.price,
                                    orderid=order_id,
                                    transactiontype=exit_transaction_type,
                                    exchange=trade.exchange,
                                    ordertype=trade.ordertype,
                                    producttype=trade.producttype,
                                    duration=trade.duration,
                                    datetime=datetime.now(),
                                    uniqueorderid=uniqueorderid,
                                    lot_size=trade.lot_size
                                )
                                db.add(new_trade)
                                db.commit()
                                logger.info(f"Trade recorded successfully for order ID {order_id}")
                                results.append({"instrument": trade.stock_symbol, "status": "success"})
                                break
                            except Exception as e:
                                logger.error(f"Error saving trade details: {str(e)}")
                                results.append({"instrument": trade.stock_symbol, "status": "failed", "message": "Error saving trade details"})
                                break
                        else:
                            logger.error(f"Order details not found for order ID {order_id}")
                            results.append({"instrument": trade.stock_symbol, "status": "failed", "message": "Order details not found"})
                            break
                    else:
                        logger.error(f"Unexpected response format: {response}")
                        results.append({"instrument": trade.stock_symbol, "status": "failed", "message": "Unexpected response format"})
                        break
                except Exception as e:
                    logger.error(f"Exception in place_order on attempt {attempt + 1}: {str(e)}")
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff

        return {"user_id": user.user_id, "status": "success", "results": results}
    except Exception as e:
        logger.error(f"Error in process_student_pending_orders for user {user.user_id}: {str(e)}")
        return {"user_id": user.user_id, "status": "failed", "message": str(e)}
    finally:
        try:
            obj.terminateSession(broker_credentials.client_id)
            logger.info(f"Session terminated for user {user.user_id}")
        except Exception as e:
            logger.error(f"Error terminating session for user {user.user_id}: {str(e)}")

#exit_all_student_pending
@app.post("/exit_all_student_pending/")
async def exit_all_student_pending(request: ExitPendingRequest, db: Session = Depends(get_db)):
    try:
        users = await fetch_users(db, request.teacher_id)
        for user in users:
            if user.broker != "angle_one":
                logger.info(f"Skipping user {user.name} as their broker is not 'angle_one'")
                continue

        if not users:
            raise HTTPException(status_code=404, detail="No active users found")

        results = []
        for user in users:
            trades = db.query(TradeBookLive).filter(
                TradeBookLive.user_id == user.user_id,
                TradeBookLive.orderid.isnot(None)
            ).all()
            user_result = await process_student_pending_orders(user, trades, db)
            results.append(user_result)

        return {"st": 1, "results": results, "msg":"Exit Order placed on accounts"}
    except Exception as e:
        logger.error(f"Error in exit_all_student_pending: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

#exit_student_instrument
@app.post("/exit_student_instrument/")
async def exit_student_instrument(request: ExitStudentInstrumentRequest, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.user_id == request.student_id).first()
    for user in user:
        if user.broker != "angle_one":
            logger.info(f"Skipping user {user.name} as their broker is not 'angle_one'")
            continue

    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    trade = db.query(TradeBookLive).filter(
        TradeBookLive.user_id == user.user_id,
        TradeBookLive.stock_symbol == request.instrument_data['tradingsymbol'],
        TradeBookLive.stock_token == request.instrument_data['symboltoken']
    ).first()

    if not trade:
        raise HTTPException(status_code=404, detail="Trade not found")

    result = await process_student_pending_orders(user, [trade], db)
    return {"st": 1, "results": result, "msg":"Exit Order placed on accounts"}

#exit_students_all_instruments
@app.post("/exit_students_all_instrument/")
async def exit_students_all_instrument(request: ExitStudentAllInstrumentsRequest, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.user_id == request.student_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    trades = db.query(TradeBookLive).filter(TradeBookLive.user_id == user.user_id).all()
    result = await process_student_pending_orders(user, trades, db)
    return {"st": 1, "results": result, "msg":"All Exit placed successfully"}

#exit_position
@app.post("/exit_position/")
async def exit_position(request: ExitPositionRequest, db: Session = Depends(get_db)):
    users = await fetch_users(db, request.teacher_id)
    if not users:
        raise HTTPException(status_code=404, detail="No active users found")

    results = []
    for user in users:
        if user.broker != "angle_one":
            logger.info(f"Skipping user {user.name} as their broker is not 'angle_one'")
            continue

    for user in users:
        user_results = []
        for order in request.order_data:
            trade = db.query(TradeBookLive).filter(
                TradeBookLive.user_id == user.user_id,
                TradeBookLive.stock_symbol == order['instrument']
            ).first()
            if trade:
                result = await process_student_pending_orders(user, [trade], db)
                user_results.append(result)
        results.append({"user_id": user.user_id, "results": user_results})

    return {"st": 1, "results": result, "msg":"Exit Order placed on accounts"}
