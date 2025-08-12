# Account Analysis Script for Interactive Brokers
#
# This script retrieves account information and calculates leverage ratio
# Based on the working examples from temp_2fix.py and IBAPIpythonexample1.py
#
# Features:
# - Get current cash balance
# - Get current positions and market values
# - Calculate net liquidation value
# - Calculate leverage ratio
# - Format output nicely

from ibapi.wrapper import EWrapper
from ibapi.client import EClient
from threading import Thread
import queue
import time
from decimal import Decimal

## marker for when queue is finished
FINISHED = object()
STARTED = object()
TIME_OUT = object()

class finishableQueue(object):
    def __init__(self, queue_to_finish):
        self._queue = queue_to_finish
        self.status = STARTED

    def get(self, timeout):
        """
        Returns a list of queue elements once timeout is finished, or a FINISHED flag is received in the queue
        :param timeout: how long to wait before giving up
        :return: list of queue elements
        """
        contents_of_queue=[]
        finished=False

        while not finished:
            try:
                current_element = self._queue.get(timeout=timeout)
                if current_element is FINISHED:
                    finished = True
                    self.status = FINISHED
                else:
                    contents_of_queue.append(current_element)
                    ## keep going and try and get more data

            except queue.Empty:
                ## If we hit a time out it's most probable we're not getting a finished element any time soon
                ## give up and return what we have
                finished = True
                self.status = TIME_OUT

        return contents_of_queue

    def timed_out(self):
        return self.status is TIME_OUT

class AccountWrapper(EWrapper):
    """
    The wrapper deals with the action coming back from the IB gateway or TWS instance
    We override methods in EWrapper that will get called when this action happens
    """

    def __init__(self):
        self._account_values = {}
        self._account_summary = {}
        self._positions = []
        self._portfolio = []

    ## error handling code
    def init_error(self):
        error_queue=queue.Queue()
        self._my_errors = error_queue

    def get_error(self, timeout=5):
        if self.is_error():
            try:
                return self._my_errors.get(timeout=timeout)
            except queue.Empty:
                return None
        return None

    def is_error(self):
        an_error_if=not self._my_errors.empty()
        return an_error_if

    def error(self, id, errorCode, errorString, advancedOrderRejectJson=""):
        ## Overriden method
        errormsg = "IB error id %d errorcode %d string %s" % (id, errorCode, errorString)
        self._my_errors.put(errormsg)

    ## Account values code
    def init_account_values(self):
        self._account_values = {}
        account_values_queue = queue.Queue()
        self._account_values_queue = account_values_queue
        return account_values_queue

    def updateAccountValue(self, key, val, currency, accountName):
        ## Overriden method - called for each account value
        print(f"DEBUG: Account Value - {key}: {val} {currency}")
        
        if key not in self._account_values:
            self._account_values[key] = {}
        
        if currency not in self._account_values[key]:
            self._account_values[key][currency] = []
            
        self._account_values[key][currency].append(val)

    def accountDownloadEnd(self, accountName):
        ## Overriden method - called when account download is complete
        if hasattr(self, '_account_values_queue'):
            self._account_values_queue.put(FINISHED)

    ## Positions code
    def init_positions(self):
        self._positions = []
        positions_queue = queue.Queue()
        self._positions_queue = positions_queue
        return positions_queue

    def position(self, account, contract, pos, avgCost):
        ## Overriden method - called for each position
        print(f"DEBUG: Position - {contract.symbol} ({contract.secType}): {pos} @ {avgCost}")
        
        position_info = {
            'account': account,
            'symbol': contract.symbol,
            'secType': contract.secType,
            'exchange': contract.exchange,
            'currency': contract.currency,
            'position': pos,
            'avgCost': avgCost
        }
        self._positions.append(position_info)

    def positionEnd(self):
        ## Overriden method - called when positions download is complete
        if hasattr(self, '_positions_queue'):
            self._positions_queue.put(FINISHED)

    ## Portfolio code
    def init_portfolio(self):
        self._portfolio = []
        portfolio_queue = queue.Queue()
        self._portfolio_queue = portfolio_queue
        return portfolio_queue

    def updatePortfolio(self, contract, position, marketPrice, marketValue, averageCost, unrealizedPNL, realizedPNL, accountName):
        ## Overriden method - called for each portfolio item
        portfolio_info = {
            'symbol': contract.symbol,
            'secType': contract.secType,
            'exchange': contract.exchange,
            'currency': contract.currency,
            'position': position,
            'marketPrice': marketPrice,
            'marketValue': marketValue,
            'averageCost': averageCost,
            'unrealizedPNL': unrealizedPNL,
            'realizedPNL': realizedPNL,
            'accountName': accountName
        }
        self._portfolio.append(portfolio_info)

    def portfolioDownloadEnd(self, accountName):
        ## Overriden method - called when portfolio download is complete
        if hasattr(self, '_portfolio_queue'):
            self._portfolio_queue.put(FINISHED)

class AccountClient(EClient):
    """
    The client method
    We don't override native methods, but instead call them from our own wrappers
    """
    def __init__(self, wrapper):
        ## Set up with a wrapper inside
        EClient.__init__(self, wrapper)

    def get_account_values(self):
        """
        Get all account values (cash, net liquidation, etc.)
        :returns dictionary of account values
        """
        print("Getting account values...")
        
        ## Make a place to store the data we're going to return
        account_values_queue = self.wrapper.init_account_values()
        
        ## Request account updates
        self.reqAccountUpdates(True, "")
        
        ## Wait for data
        MAX_WAIT_SECONDS = 15
        try:
            account_values = account_values_queue.get(timeout=MAX_WAIT_SECONDS)
        except queue.Empty:
            print("Account values download timed out")
            account_values = {}
        
        while self.wrapper.is_error():
            print(self.wrapper.get_error())
        
        # Return the actual stored data, not the queue
        return self.wrapper._account_values

    def get_portfolio(self):
        """
        Get portfolio information (market values, P&L, etc.)
        :returns list of portfolio dictionaries
        """
        print("Getting portfolio information...")
        
        ## Make a place to store the data we're going to return
        portfolio_queue = self.wrapper.init_portfolio()
        
        ## Wait for portfolio data to come through the account updates
        MAX_WAIT_SECONDS = 15
        try:
            portfolio = portfolio_queue.get(timeout=MAX_WAIT_SECONDS)
        except queue.Empty:
            print("Portfolio download timed out")
            portfolio = []
        
        while self.wrapper.is_error():
            print(self.wrapper.get_error())
        
        return portfolio

    def get_positions(self):
        """
        Get current positions
        :returns list of position dictionaries
        """
        print("Getting current positions...")
        
        ## Make a place to store the data we're going to return
        positions_queue = self.wrapper.init_positions()
        
        ## Request positions
        self.reqPositions()
        
        ## Wait for data
        MAX_WAIT_SECONDS = 15
        try:
            positions = positions_queue.get(timeout=MAX_WAIT_SECONDS)
        except queue.Empty:
            print("Positions download timed out")
            positions = []
        
        while self.wrapper.is_error():
            print(self.wrapper.get_error())
        
        # Return the actual stored data, not the queue
        return self.wrapper._positions



class AccountApp(AccountWrapper, AccountClient):
    def __init__(self, ipaddress, portid, clientid):
        AccountWrapper.__init__(self)
        AccountClient.__init__(self, wrapper=self)

        self.connect(ipaddress, portid, clientid)

        thread = Thread(target = self.run)
        thread.start()

        setattr(self, "_thread", thread)

        self.init_error()

def format_currency(value, currency="USD"):
    """Format currency values nicely"""
    try:
        if isinstance(value, str):
            value = float(value)
        return f"${value:,.2f}" if currency == "USD" else f"{value:,.2f} {currency}"
    except:
        return f"{value} {currency}"

def calculate_leverage_ratio(total_market_value, net_liquidation_value):
    """Calculate leverage ratio"""
    if net_liquidation_value == 0:
        return "N/A (No equity)"
    
    leverage = total_market_value / net_liquidation_value
    return f"{leverage:.2f}x"

def analyze_account(app):
    """Main function to analyze the account"""
    
    print("=" * 60)
    print("ACCOUNT ANALYSIS")
    print("=" * 60)
    
    # Get account values
    account_values = app.get_account_values()
    
    # Get positions
    positions = app.get_positions()
    
    # Get portfolio
    portfolio = app.get_portfolio()
    
    # Extract key values
    cash_balance = 0
    net_liquidation = 0
    total_market_value = 0
    
    # Parse account values
    if isinstance(account_values, dict):
        for key, currencies in account_values.items():
            if key == "NetLiquidation":
                for currency, values in currencies.items():
                    if currency == "USD":
                        net_liquidation = sum(float(v) for v in values)
            elif key == "TotalCashValue":
                for currency, values in currencies.items():
                    if currency == "USD":
                        cash_balance = sum(float(v) for v in values)
            elif key == "StockMarketValue":
                for currency, values in currencies.items():
                    if currency == "USD":
                        total_market_value = sum(float(v) for v in values)
    else:
        print("No account values received")
    
    # Market value is now obtained directly from IB's StockMarketValue
    # No need to calculate from positions
    if isinstance(positions, list):
        print(f"DEBUG: Found {len([p for p in positions if p != FINISHED])} positions")
    else:
        print("No positions received")
    
    # Calculate leverage ratio
    leverage_ratio = calculate_leverage_ratio(total_market_value, net_liquidation)
    
    # Format output
    print(f"\n💰 CASH BALANCE: {format_currency(cash_balance)}")
    print(f"📊 NET LIQUIDATION VALUE: {format_currency(net_liquidation)}")
    print(f"📈 TOTAL MARKET VALUE: {format_currency(total_market_value)}")
    print(f"⚖️  LEVERAGE RATIO: {leverage_ratio}")
    
    print(f"\n📋 POSITIONS ({len([p for p in positions if p != FINISHED]) if isinstance(positions, list) else 0}):")
    print("-" * 60)
    
    if isinstance(positions, list):
        for position in positions:
            if position != FINISHED:
                print(f"  {position['symbol']} ({position['secType']}) - {position['position']} @ {format_currency(position['avgCost'])}")
    else:
        print("  No positions available")
    
    print(f"\n📊 PORTFOLIO SUMMARY:")
    print("-" * 60)
    
    if isinstance(portfolio, list):
        total_unrealized_pnl = 0
        for item in portfolio:
            if item != FINISHED:
                total_unrealized_pnl += float(item['unrealizedPNL'])
                print(f"  {item['symbol']}: {format_currency(item['marketValue'])} | P&L: {format_currency(item['unrealizedPNL'])}")
        
        print(f"\n💵 TOTAL UNREALIZED P&L: {format_currency(total_unrealized_pnl)}")
    else:
        print("  No portfolio data available")
    
    print("\n" + "=" * 60)
    
    return {
        'cash_balance': cash_balance,
        'net_liquidation': net_liquidation,
        'total_market_value': total_market_value,
        'leverage_ratio': leverage_ratio,
        'positions': positions,
        'portfolio': portfolio
    }

if __name__ == '__main__':
    
    # Create connection
    app = AccountApp("127.0.0.1", 4001, 1)

    # Wait for connection and check status
    print("Waiting for connection to establish...")
    time.sleep(3)

    # Check connection status
    if not app.isConnected():
        print("ERROR: Not connected to IB server!")
        app.disconnect()
        exit(1)

    print("Successfully connected to IB server!")
    print("Connection status:", app.isConnected())

    try:
        # Analyze the account
        results = analyze_account(app)
        
        print(f"\n✅ Analysis complete!")
        print(f"Summary: Cash: {format_currency(results['cash_balance'])}, "
              f"Net Liq: {format_currency(results['net_liquidation'])}, "
              f"Leverage: {results['leverage_ratio']}")
              
    except Exception as e:
        print(f"❌ Error during analysis: {e}")
    
    finally:
        # Clean up
        app.disconnect()
        print("Disconnected from IB server")
