--CREATE TYPE taxStatus AS Edecimal ('0', '1', '2');
use tpcdi;
CREATE external TABLE DimBroker  (
							SK_BrokerID  INTEGER,
							BrokerID  INTEGER,
							ManagerID  INTEGER,
							FirstName       CHAR(50),
							LastName       CHAR(50),
							MiddleInitial       CHAR(1),
							Branch       CHAR(50),
							Office       CHAR(50),
							Phone       CHAR(14),
							IsCurrent boolean,
							BatchID INTEGER,
							EffectiveDate date,
							EndDate date							
);

CREATE external TABLE DimCustomer  (
							SK_CustomerID  INTEGER,
							CustomerID INTEGER,
							TaxID CHAR(20),
							Status CHAR(10),
							LastName CHAR(30),
							FirstName CHAR(30),
							MiddleInitial CHAR(1),
							Gender CHAR(1),
							Tier Integer,
							DOB timestamp,
							AddressLine1	varchar(80),
							AddressLine2	varchar(80),
							PostalCode	char(12),
							City	char(25),
							StateProv	char(20),
							Country	char(24),
							Phone1	char(30),
							Phone2	char(30),
							Phone3	char(30),
							Email1	char(50),
							Email2	char(50),
							NationalTaxRateDesc	varchar(50),
							NationalTaxRate	decimal(6,5),
							LocalTaxRateDesc	varchar(50),
							LocalTaxRate	decimal(6,5),
							AgencyID	char(30),
							CreditRating integer,
							NetWorth	decimal(10),
							MarketingNameplate varchar(100),
							IsCurrent boolean,
							BatchID INTEGER,
							EffectiveDate timestamp,
							EndDate timestamp 
);

CREATE TABLE DimAccount  ( SK_AccountID  INTEGER NOT NULL PRIMARY KEY,
                            AccountID  INTEGER NOT NULL,
                            SK_BrokerID  INTEGER NOT NULL REFERENCES DimBroker (SK_BrokerID),
                            SK_CustomerID  INTEGER NOT NULL REFERENCES DimCustomer (SK_CustomerID),
                            Status       CHAR(10) NOT NULL,
                            AccountDesc       varchar(50),
                            TaxStatus  INTEGER NOT NULL CHECK (TaxStatus = 0 OR TaxStatus = 1 OR TaxStatus = 2),
                            IsCurrent boolean NOT NULL,
                            BatchID INTEGER NOT NULL,
                            EffectiveDate date NOT NULL,
                            EndDate date NOT NULL
);


CREATE TABLE DimCompany (   SK_CompanyID INTEGER NOT NULL PRIMARY KEY, 
							CompanyID INTEGER NOT NULL,
							Status CHAR(10) Not NULL, 
							Name CHAR(60) Not NULL,
							Industry CHAR(50) Not NULL,
							SPrating CHAR(4),
							isLowGrade BOOLEAN,
							CEO CHAR(100) Not NULL,
							AddressLine1 CHAR(80),
							AddressLine2 CHAR(80),
							PostalCode CHAR(12) Not NULL,
							City CHAR(25) Not NULL,
							StateProv CHAR(20) Not NULL,
							Country CHAR(24),
							Description CHAR(150) Not NULL,
							FoundingDate DATE,
							IsCurrent BOOLEAN Not NULL,
							BatchID decimal(5) Not NULL,
							EffectiveDate DATE Not NULL,
							EndDate DATE Not NULL
);

CREATE TABLE DimDate (  SK_DateID INTEGER Not NULL PRIMARY KEY,
						DateValue DATE Not NULL,
						DateDesc CHAR(20) Not NULL,
						CalendarYearID decimal(4) Not NULL,
						CalendarYearDesc CHAR(20) Not NULL,
						CalendarQtrID decimal(5) Not NULL,
						CalendarQtrDesc CHAR(20) Not NULL,
						CalendarMonthID decimal(6) Not NULL,
						CalendarMonthDesc CHAR(20) Not NULL,
						CalendarWeekID decimal(6) Not NULL,
						CalendarWeekDesc CHAR(20) Not NULL,
						DayOfWeekdecimal decimal(1) Not NULL,
						DayOfWeekDesc CHAR(10) Not NULL,
						FiscalYearID decimal(4) Not NULL,
						FiscalYearDesc CHAR(20) Not NULL,
						FiscalQtrID decimal(5) Not NULL,
						FiscalQtrDesc CHAR(20) Not NULL,
						HolidayFlag BOOLEAN
);

CREATE TABLE DimSecurity( SK_SecurityID INTEGER Not NULL PRIMARY KEY,
							Symbol CHAR(15) Not NULL,
							Issue CHAR(6) Not NULL,
							Status CHAR(10) Not NULL,
							Name CHAR(70) Not NULL,
							ExchangeID CHAR(6) Not NULL,
							SK_CompanyID INTEGER Not NULL REFERENCES DimCompany (SK_CompanyID),
							SharesOutstanding INTEGER Not NULL,
							FirstTrade DATE Not NULL,
							FirstTradeOnExchange DATE Not NULL,
							Dividend INTEGER Not NULL,
							IsCurrent BOOLEAN Not NULL,
							BatchID decimal(5) Not NULL,
							EffectiveDate DATE Not NULL,
							EndDate DATE Not NULL
);

CREATE TABLE DimTime ( SK_TimeID INTEGER Not NULL PRIMARY KEY,
						TimeValue TIME Not NULL,
						HourID decimal(2) Not NULL,
						HourDesc CHAR(20) Not NULL,
						MinuteID decimal(2) Not NULL,
						MinuteDesc CHAR(20) Not NULL,
						SecondID decimal(2) Not NULL,
						SecondDesc CHAR(20) Not NULL,
						MarketHoursFlag BOOLEAN,
						OfficeHoursFlag BOOLEAN
);

CREATE TABLE DimTrade (	TradeID INTEGER Not NULL,
						SK_BrokerID INTEGER REFERENCES DimBroker (SK_BrokerID),
						SK_CreateDateID INTEGER Not NULL REFERENCES DimDate (SK_DateID),
						SK_CreateTimeID INTEGER Not NULL REFERENCES DimTime (SK_TimeID),
						SK_CloseDateID INTEGER REFERENCES DimDate (SK_DateID),
						SK_CloseTimeID INTEGER REFERENCES DimTime (SK_TimeID),
						Status CHAR(10) Not NULL,
						DT_Type CHAR(12) Not NULL,
						CashFlag BOOLEAN Not NULL,
						SK_SecurityID INTEGER Not NULL REFERENCES DimSecurity (SK_SecurityID),
						SK_CompanyID INTEGER Not NULL REFERENCES DimCompany (SK_CompanyID),
						Quantity decimal(6,0) Not NULL,
						BidPrice decimal(8,2) Not NULL,
						SK_CustomerID INTEGER Not NULL REFERENCES DimCustomer (SK_CustomerID),
						SK_AccountID INTEGER Not NULL REFERENCES DimAccount (SK_AccountID),
						ExecutedBy CHAR(64) Not NULL,
						TradePrice decimal(8,2),
						Fee decimal(10,2),
						Commission decimal(10,2),
						Tax decimal(10,2),
						BatchID decimal(5) Not Null
);

CREATE TABLE DImessages ( MessageDateAndTime TIMESTAMP Not NULL,
							BatchID decimal(5) Not NULL,
							MessageSource CHAR(30),
							MessageText CHAR(50) Not NULL,
							MessageType CHAR(12) Not NULL,
							MessageData CHAR(100)
);

CREATE TABLE FactCashBalances ( SK_CustomerID INTEGER Not Null REFERENCES DimCustomer (SK_CustomerID),
								SK_AccountID INTEGER Not Null REFERENCES DimAccount (SK_AccountID),
								SK_DateID INTEGER Not Null REFERENCES DimDate (SK_DateID),
								Cash decimal(15,2) Not Null,
								BatchID decimal(5)
);

CREATE TABLE FactHoldings (	TradeID INTEGER Not NULL,
							CurrentTradeID INTEGER Not Null,
							SK_CustomerID INTEGER Not NULL REFERENCES DimCustomer (SK_CustomerID),
							SK_AccountID INTEGER Not NULL REFERENCES DimAccount (SK_AccountID),
							SK_SecurityID INTEGER Not NULL REFERENCES DimSecurity (SK_SecurityID),
							SK_CompanyID INTEGER Not NULL REFERENCES DimCompany (SK_CompanyID),
							SK_DateID INTEGER Not NULL REFERENCES DimDate (SK_DateID),
							SK_TimeID INTEGER Not NULL REFERENCES DimTime (SK_TimeID),
							CurrentPrice INTEGER CHECK (CurrentPrice > 0) ,
							CurrentHolding decimal(6) Not NULL,
							BatchID decimal(5)
);

CREATE TABLE FactMarketHistory (    SK_SecurityID INTEGER Not Null REFERENCES DimSecurity (SK_SecurityID),
									SK_CompanyID INTEGER Not Null REFERENCES DimCompany (SK_CompanyID),
									SK_DateID INTEGER Not Null REFERENCES DimDate (SK_DateID),
									PERatio decimal(10,2),
									Yield decimal(5,2) Not Null,
									FiftyTwoWeekHigh decimal(8,2) Not Null,
									SK_FiftyTwoWeek INTEGER Not Null,
									FiftyTwoWeekLow decimal(8,2) Not Null,
									SK_FiftyTwoWeekL INTEGER Not Null,
									ClosePrice decimal(8,2) Not Null,
									DayHigh decimal(8,2) Not Null,
									DayLow decimal(8,2) Not Null,
									Volume decimal(12) Not Null,
									BatchID decimal(5)
);

CREATE TABLE FactWatches ( SK_CustomerID INTEGER Not NULL REFERENCES DimCustomer (SK_CustomerID),
							SK_SecurityID INTEGER Not NULL REFERENCES DimSecurity (SK_SecurityID),
							SK_DateID_DatePlaced INTEGER Not NULL REFERENCES DimDate (SK_DateID),
							SK_DateID_DateRemoved INTEGER REFERENCES DimDate (SK_DateID),
							BatchID decimal(5) Not Null 
);

CREATE TABLE Industry ( IN_ID CHAR(2) Not NULL,
						IN_NAME CHAR(50) Not NULL,
						IN_SC_ID CHAR(4) Not NULL
);

CREATE TABLE Financial ( SK_CompanyID INTEGER Not NULL REFERENCES DimCompany (SK_CompanyID),
						FI_YEAR decimal(4) Not NULL,
						FI_QTR decimal(1) Not NULL,
						FI_QTR_START_DATE DATE Not NULL,
						FI_REVENUE decimal(15,2) Not NULL,
						FI_NET_EARN decimal(15,2) Not NULL,
						FI_BASIC_EPS decimal(10,2) Not NULL,
						FI_DILUT_EPS decimal(10,2) Not NULL,
						FI_MARGIN decimal(10,2) Not NULL,
						FI_INVENTORY decimal(15,2) Not NULL,
						FI_ASSETS decimal(15,2) Not NULL,
						FI_LIABILITY decimal(15,2) Not NULL,
						FI_OUT_BASIC decimal(12) Not NULL,
						FI_OUT_DILUT decimal(12) Not NULL
);

CREATE TABLE Prospect ( 
						AgencyID CHAR(30),  
						SK_RecordDateID INTEGER , 
						SK_UpdateDateID INTEGER  REFERENCES DimDate (SK_DateID),
						BatchID decimal(5) ,
						IsCustomer BOOLEAN ,
						LastName CHAR(30) ,
						FirstName CHAR(30) ,
						MiddleInitial CHAR(1),
						Gender CHAR(1),
						AddressLine1 CHAR(80),
						AddressLine2 CHAR(80),
						PostalCode CHAR(12),
						City CHAR(25) ,
						State CHAR(20) ,
						Country CHAR(24),
						Phone CHAR(30), 
						Income decimal(9),
						NumberCars decimal(2), 
						NumberChildren decimal(2), 
						MaritalStatus CHAR(1), 
						Age decimal(3),
						CreditRating decimal(4),
						OwnOrRentFlag CHAR(1), 
						Employer CHAR(30),
						NumberCreditCards decimal(2), 
						NetWorth decimal(12),
						MarketingNameplate CHAR(100)
);

CREATE TABLE StatusType ( ST_ID CHAR(4) Not NULL,
							ST_NAME CHAR(10) Not NULL
);

CREATE TABLE TaxRate ( TX_ID CHAR(4) Not NULL,
						TX_NAME CHAR(50) Not NULL,
						TX_RATE decimal(6,5) Not NULL
);

CREATE TABLE TradeType ( TT_ID CHAR(3) Not NULL,
							TT_NAME CHAR(12) Not NULL,
							TT_IS_SELL decimal(1) Not NULL,
							TT_IS_MRKT decimal(1) Not NULL
);

CREATE TABLE AuditTable ( DataSet CHAR(20) Not Null,
							BatchID decimal(5),
							AT_Date DATE,
							AT_Attribute CHAR(50),
							AT_Value decimal(15),
							DValue decimal(15,5)
);

CREATE INDEX PIndex ON dimtrade (tradeid);
CREATE TABLE dimtradeforexperiment
(
  tradeid integer NOT NULL,
  sk_brokerid integer,
  date_int integer,
  time_int integer,
  status character(10) NOT NULL,
  dt_type character(12) NOT NULL,
  cashflag boolean NOT NULL,
  sk_securityid integer NOT NULL,
  sk_companyid integer NOT NULL,
  quantity decimal(6,0) NOT NULL,
  bidprice decimal(8,2) NOT NULL,
  sk_customerid integer NOT NULL,
  sk_accountid integer NOT NULL,
  executedby character(64) NOT NULL,
  tradeprice decimal(8,2),
  fee decimal(10,2),
  commission decimal(10,2),
  tax decimal(10,2),
  batchid decimal(5,0) NOT NULL,
  th_st_id character(4)
);


