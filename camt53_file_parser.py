"""
This module provides classes and functions for parsing and representing bank statements in the CAMT.053 format.

Classes:
    - CreditOrDebit (Enum): Enumeration for credit and debit types.
    - BankId (BaseModel): Represents a bank identifier.
    - AccountId (BaseModel): Represents an account identifier.
    - TransactionRef (BaseModel): Represents reference details for a financial transaction.
    - Balance (BaseModel): Represents the balance information.
    - Transaction (BaseModel): Represents a financial transaction.
    - BankToCustomerStatement (BaseModel): Represents a bank statement in the CAMT.053 format.

Functions:
    - parse_statement(stmt: _Element) -> BankToCustomerStatement: Parse an XML statement element into a BankToCustomerStatement object.
    - parse_transactions(stmt: _Element) -> List[Transaction]: Parse XML transactions elements into a list of Transaction objects.
    - parse_transaction(ntry: _Element) -> Transaction: Parse an XML transaction element into a Transaction object.
    - parse_date_isoformat(s: str) -> datetime.date: Parse an ISO-formatted date string into a datetime.date object.
    - flatten_dict(d: Dict[str, Any], prefix: str = "") -> Dict[str, Any]: Flatten a nested dictionary with a specified prefix.
    - parse_xml(input_xml: str) -> List[BankToCustomerStatement]: Parse an XML string into a list of BankToCustomerStatement objects.
    - parse_statements_to_dataframe(xml_content: str) -> pd.DataFrame: Parse XML content containing bank statements and return a DataFrame.
    - main(input_xml: str) -> List[BankToCustomerStatement]: Main function to parse XML input and return a list of BankToCustomerStatement objects.
"""
from typing import Optional, Any, List, Dict
from lxml import etree
from lxml.etree import _Element
from io import BytesIO
from pydantic import BaseModel
from enum import Enum
import datetime
from decimal import Decimal
import warnings

try:
    import pandas as pd
except ImportError:
    print("Warning: pandas is not installed. The output will be a list of dictionaries")
    pd = None  # type: ignore[assignment]


def get_text_or_none(
    e: Optional[_Element] = None, path: Optional[str] = None, strip: bool = True
) -> Optional[str]:
    """
    Get the text content of an XML element or None if the element or the specified path is not found.

    Args:
                    e (Optional[_Element]): The XML element.
                    path (Optional[str]): The XPath expression to find the desired subelement.
                    strip (bool): Whether to strip leading and trailing whitespaces from the text.

    Returns:
                    Optional[str]: The text content of the element or None if not found.
    """
    if e is None:
        return None
    else:
        if path is not None:
            e = e.find(path)

        if e is None:
            return None
        else:
            text = e.text
            if strip and text is not None:
                text = text.strip()
            return text


def get_text(
    e: Optional[_Element] = None, path: Optional[str] = None, strip: bool = True
) -> str:
    """
    Get the text content of an XML element, raising a ValueError if the element or the specified path is not found.

    Args:
                    e (Optional[_Element]): The XML element.
                    path (Optional[str]): The XPath expression to find the desired subelement.
                    strip (bool): Whether to strip leading and trailing whitespaces from the text.

    Returns:
                    str: The text content of the element.

    Raises:
                    ValueError: If the element or the specified path is not found.
    """
    text = get_text_or_none(e, path, strip)
    if text is None:
        raise ValueError("Missing mandatory element")
    return text


def get_element(root: _Element, path: str) -> _Element:
    """
    Get a subelement from the root element using the specified XPath expression, raising a ValueError if not found.

    Args:
                    root (_Element): The root XML element.
                    path (str): The XPath expression to find the desired subelement.

    Returns:
                    _Element: The found subelement.

    Raises:
                    ValueError: If the subelement is not found.
    """
    e = root.find(path)
    if e is None:
        raise ValueError(f"Missing mandatory element ({path})")
    return e


def get_attribute(e: _Element, attr: str) -> str:
    """
    Get the value of an attribute from an XML element.

    Args:
                    e (_Element): The XML element.
                    attr (str): The name of the attribute.

    Returns:
                    str: The value of the attribute.
    """
    value = e.attrib[attr]
    return str(value)


class CreditOrDebit(str, Enum):
    """
    Enumeration representing credit and debit types.

    Enumeration Values:
                    - CRDT: Credit type.
    """

    CRDT = "CRDT"
    DBIT = "DBIT"


class BankId(BaseModel):
    """
    Represents a bank identifier.

                Attributes:
                                bic (Optional[str]): The Bank Identifier Code (BIC) of the bank.
                                id (Optional[str]): An additional identifier for the bank.

                Methods:
                                __str__() -> str: Get a string representation of the BankId.
                                from_xml(root: Optional[_Element]) -> Optional[BankId]: Create a BankId instance from an XML element.
    """

    bic: Optional[str] = None
    id: Optional[str] = None

    def __str__(self) -> str:
        """
        Get a string representation of the BankId.

        Returns:
            str: The string representation.
        """
        return self.bic or self.id or ""

    @classmethod
    def from_xml(cls, root: Optional[_Element]) -> Optional["BankId"]:
        """
        Create a BankId instance from an XML element.

        Args:
                        cls: The class.
                        root (Optional[_Element]): The XML element.

        Returns:
                        Optional["BankId"]: The BankId instance or None if no relevant information is found.
        """
        bic = get_text_or_none(root, "BIC") or get_text_or_none(root, "BICFI")
        id = get_text_or_none(root, "Othr/Id")

        if bic or id:
            return cls(
                bic=bic,
                id=id,
            )
        else:
            return None


class AccountId(BaseModel):
    """
    Represents an account identifier.

    Attributes:
                    iban (Optional[str]): The International Bank Account Number (IBAN) of the account.
                    id (Optional[str]): An additional identifier for the account.

    Methods:
                    __str__() -> str: Get a string representation of the AccountId.
                    from_xml(root: Optional[_Element]) -> Optional[AccountId]: Create an AccountId instance from an XML element.
    """

    iban: Optional[str] = None
    id: Optional[str] = None

    def __str__(self) -> str:
        """
        Get a string representation of the AccountId.

        Returns:
                        str: The string representation.
        """
        return self.iban or self.id or ""

    @classmethod
    def from_xml(cls, root: Optional[_Element]) -> Optional["AccountId"]:
        """
        Create an AccountId instance from an XML element.

        Args:
                        cls: The class.
                        root (Optional[_Element]): The XML element.

        Returns:
                        Optional["AccountId"]: The AccountId instance or None if no relevant information is found.
        """
        iban = get_text_or_none(root, "IBAN")
        id = get_text_or_none(root, "Othr/Id")

        if iban or id:
            return cls(
                iban=iban,
                id=id,
            )
        else:
            return None


class TransactionRef(BaseModel):
    """
    Represents reference details for a financial transaction.

    Attributes:
                    message_id (Optional[str]): The unique identifier for the entire message.
                    end_to_end_id (Optional[str]): The end-to-end identification assigned by the instructing party.
                    account_servicer_ref (Optional[str]): Reference assigned by the account servicing institution.
                    payment_invocation_id (Optional[str]): Identifies the payment instruction within the message.
                    instruction_id (Optional[str]): Unique identification assigned by the instructing party for the instruction.
                    mandate_id (Optional[str]): Unique identification assigned by the creditor to unambiguously identify a mandate.
                    cheque_number (Optional[str]): Number of the cheque used for the transaction.
                    clearing_system_ref (Optional[str]): Reference identifying the clearing system.

    Methods:
                    __str__() -> str: Get a string representation of the TransactionRef.
                    from_xml(root: Optional[_Element]) -> "TransactionRef": Create a TransactionRef instance from an XML element.
    """

    message_id: Optional[str] = None
    end_to_end_id: Optional[str] = None
    account_servicer_ref: Optional[str] = None
    payment_invocation_id: Optional[str] = None
    instruction_id: Optional[str] = None
    mandate_id: Optional[str] = None
    cheque_number: Optional[str] = None
    clearing_system_ref: Optional[str] = None

    def __str__(self) -> str:
        """
        Get a string representation of the TransactionRef.

        Returns:
                        str: A string with key-value pairs of non-None attributes.
        """
        return ", ".join(
            f"{k}={v}" for k, v in self.model_dump().items() if v is not None
        )

    @classmethod
    def from_xml(cls, root: Optional[_Element]) -> "TransactionRef":
        """
        Create a TransactionRef instance from an XML element.

        Args:
                        cls: The class.
                        root (Optional[_Element]): The XML element.

        Returns:
                        "TransactionRef": The TransactionRef instance.
        """
        if root is None:
            return cls()
        else:
            return cls(
                message_id=get_text_or_none(root, "MsgId"),
                account_servicer_ref=get_text_or_none(root, "AcctSvcrRef"),
                payment_invocation_id=get_text_or_none(root, "PmtInfId"),
                instruction_id=get_text_or_none(root, "InstrId"),
                end_to_end_id=get_text_or_none(root, "EndToEndId"),
                mandate_id=get_text_or_none(root, "MndtId"),
                cheque_number=get_text_or_none(root, "ChqNb"),
                clearing_system_ref=get_text_or_none(root, "ClrSysRef"),
            )


class Balance(BaseModel):
    """
    Represents the balance of an account at a specific date.

    Attributes:
                    amount (Decimal): The amount of the balance.
                    currency (str): The currency code of the balance.
                    date (datetime.date): The date of the balance.
    """

    amount: Decimal
    currency: str
    date: datetime.date


class Transaction(BaseModel):
    """
    Represents a financial transaction.

    Attributes:
                    ref (TransactionRef): Reference details for the transaction.
                    entry_ref (str): Reference identifier for the transaction entry.
                    amount (Decimal): The amount of the transaction.
                    currency (str): The currency code of the transaction.
                    operation (str): The type of operation, either 'debit' or 'credit'.
                    val_date (datetime.date): The valuation date of the transaction.
                    book_datetime (str): The tranaction book timestamp.
                    bai_code (str): Specifies the BAI code of the transaction.
                    remote_info (Optional[str]): Additional remote information related to the transaction.
                    additional_transaction_info (Optional[str]): Additional information about the transaction.
                    related_account_id (Optional[AccountId]): The related account's identifier.
                    related_account_bank_id (Optional[BankId]): The related account's bank identifier.

    Properties:
                    info (str): Get a formatted string containing remote information and additional transaction info.
                    related_account (Optional[str]): Get a formatted string representing the related account information.
    """

    ref: TransactionRef
    entry_ref: str
    amount: Decimal
    currency: str
    operation: str
    val_date: datetime.date
    book_datetime: str
    bai_code: Optional[str] = None
    remote_info: Optional[str] = None
    additional_transaction_info: Optional[str] = None
    related_account_id: Optional[AccountId] = None
    related_account_bank_id: Optional[BankId] = None

    @property
    def info(self) -> str:
        """
        Get a formatted string containing remote information and additional transaction info.

        Returns:
                        str: The formatted string.
        """
        remote_info = (self.remote_info or "").strip()
        additional_transaction_info = (self.additional_transaction_info or "").strip()

        if remote_info and additional_transaction_info:
            if remote_info == additional_transaction_info:
                return remote_info
            else:
                return f"{remote_info} / {additional_transaction_info}"
        else:
            return remote_info or additional_transaction_info

    @property
    def related_account(self) -> Optional[str]:
        """
        Get a formatted string representing the related account information.

        Returns:
                        Optional[str]: The formatted string or None if no related account information is present.
        """
        if self.related_account_id is None and self.related_account_bank_id is None:
            return None
        else:
            return f"{self.related_account_id}/{self.related_account_bank_id}"


class BankToCustomerStatement(BaseModel):
    """
    Represents a bank statement for a customer.

    Attributes:
                    statement_id (str): Unique identifier for the statement.
                    created_time (datetime.datetime): The date and time when the statement was created.
                    from_time (datetime.datetime): The starting date and time of the statement period.
                    to_time (datetime.datetime): The ending date and time of the statement period.
                    account_id (AccountId): The identifier of the bank account associated with the statement.
                    opening_balance (Optional[Balance]): The opening balance of the statement, if available.
                    closing_balance (Optional[Balance]): The closing balance of the statement, if available.
                    transactions (List[Transaction]): List of transactions included in the statement.

    Methods:
                    from_file(path: str) -> "BankToCustomerStatement": Create a BankToCustomerStatement instance from an XML file.
                    as_dataframe() -> pd.DataFrame: Convert the statement and its transactions into a Pandas DataFrame.

    Raises:
                    RuntimeError: If pandas is not installed.
    """

    statement_id: str
    created_time: datetime.datetime
    from_time: datetime.datetime
    to_time: datetime.datetime
    account_id: AccountId
    opening_balance: Optional[Balance] = None
    closing_balance: Optional[Balance] = None
    transactions: List[Transaction]

    @classmethod
    def from_file(cls, path: str) -> "BankToCustomerStatement":
        """
        Create a BankToCustomerStatement instance from an XML file.

        Args:
                        cls: The class.
                        path (str): The path to the XML file.

        Returns:
                        "BankToCustomerStatement": The BankToCustomerStatement instance.
        """
        with open(path, "rb") as fp:
            raw_xml = fp.read()

        raw_xml_no_namespace = raw_xml.replace(
            b'xmlns="urn:iso:std:iso:20022:tech:xsd:camt.053.001.02"', b""
        )
        tree = etree.parse(BytesIO(raw_xml_no_namespace))
        root = tree.getroot()

        return parse_statement(root)

    def as_dataframe(self) -> "pd.DataFrame":
        """
        Convert the statement and its transactions into a Pandas DataFrame.

        Returns:
                        "pd.DataFrame": The Pandas DataFrame representation of the statement.

        Raises:
                        RuntimeError: If pandas is not installed.
        """
        if pd is None:
            raise RuntimeError("pandas is not installed")

        rows = [
            flatten_dict(tx.model_dump(), prefix="transaction_")
            for tx in self.transactions
        ]
        df = pd.DataFrame.from_records(rows)
        df["statement_id"] = self.statement_id
        df["statement_account_id"] = str(self.account_id)
        df["statement_opening_balance"] = str(self.opening_balance.amount)
        df["statement_closing_balance"] = str(self.closing_balance.amount)

        return df


def parse_statement(stmt: _Element) -> BankToCustomerStatement:
    """
    Parse an XML element representing a bank statement and create a BankToCustomerStatement instance.

    Args:
                    stmt (_Element): The XML element representing the statement.

    Returns:
                    BankToCustomerStatement: The BankToCustomerStatement instance.

    Raises:
                    ValueError: If essential information is missing.
    """
    statement_id = get_text(stmt.find("Id"))
    created_time = datetime.datetime.fromisoformat(get_text(stmt, "CreDtTm"))
    from_time = datetime.datetime.fromisoformat(get_text(stmt, "FrToDt/FrDtTm"))
    to_time = datetime.datetime.fromisoformat(get_text(stmt, "FrToDt/ToDtTm"))
    account_id = AccountId.from_xml(get_element(stmt, "Acct/Id"))
    opening_balance = None
    closing_balance = None

    if account_id is None:
        raise ValueError("Missing AccountID elements")

    for bal in stmt.findall("Bal"):
        bal_date = parse_date_isoformat(get_text(bal, "Dt/Dt"))
        amt = get_element(bal, "Amt")
        bal_currency = get_attribute(amt, "Ccy")
        amount = Decimal(get_text(amt))
        tmp = CreditOrDebit(get_text(bal, "CdtDbtInd"))
        if tmp == CreditOrDebit.DBIT:
            amount *= -1
        tmp2 = get_text(bal, "Tp/CdOrPrtry/Cd")

        balance = Balance(amount=amount, currency=bal_currency, date=bal_date)

        if tmp2 == "OPBD":
            opening_balance = balance
        elif tmp2 == "CLBD":
            closing_balance = balance

    transactions = parse_transactions(stmt)
    if not transactions:
        print("No transactions found!")

    return BankToCustomerStatement(
        statement_id=statement_id,
        created_time=created_time,
        from_time=from_time,
        to_time=to_time,
        account_id=account_id,
        opening_balance=opening_balance,
        closing_balance=closing_balance,
        transactions=transactions,
    )


def parse_transactions(stmt: _Element) -> List[Transaction]:
    """
    Parse XML elements representing transactions and create a list of Transaction instances.

    Args:
                    stmt (_Element): The XML element containing transactions.

    Returns:
                    List[Transaction]: List of Transaction instances.
    """
    # print(etree.tostring(stmt, pretty_print=True).decode())
    return [parse_transaction(ntry) for ntry in stmt.findall("Ntry")]


def parse_transaction(ntry: _Element) -> Transaction:
    """
    Parse an XML element representing a transaction and create a Transaction instance.

    Args:
                    ntry (_Element): The XML element representing the transaction.

    Returns:
                    Transaction: The Transaction instance.
    """
    # print(etree.tostring(ntry, pretty_print=True).decode())
    entry_ref = get_text(ntry, "NtryRef")
    ref = TransactionRef.from_xml(ntry.find("NtryDtls/TxDtls/Refs"))

    amt = get_element(ntry, "Amt")
    currency = get_attribute(amt, "Ccy")
    amount = Decimal(get_text(amt))
    tmp = CreditOrDebit(get_text(ntry, "CdtDbtInd"))
    if tmp == CreditOrDebit.DBIT:
        operation = "debit"
    elif tmp == CreditOrDebit.CRDT:
        operation = "credit"

    val_date = parse_date_isoformat(get_text(ntry, "ValDt/Dt"))
    book_datetime = get_text_or_none(ntry, "BookgDt/DtTm")

    # Find the BAI code of transaction
    bai_code = (
        get_text_or_none(ntry, "BkTxCd/Prtry/Cd")
        if get_text_or_none(ntry, "BkTxCd/Prtry/Issr") == "BAI"
        else None
    )

    # Concatenate multiple Ustrd elements into a single string
    remote_info_elements = ntry.findall("NtryDtls/TxDtls/RmtInf/Ustrd")
    remote_info = ", ".join(
        get_text_or_none(element) for element in remote_info_elements
    )
    additional_transaction_info = get_text_or_none(ntry, "NtryDtls/TxDtls/AddtlTxInf")

    dbtr_acct_id = ntry.find("NtryDtls/TxDtls/RltdPties/DbtrAcct/Id")
    if dbtr_acct_id is not None:
        related_account_id = AccountId.from_xml(dbtr_acct_id)
    else:
        cdtr_acct_id = ntry.find("NtryDtls/TxDtls/RltdPties/CdtrAcct/Id")
        if cdtr_acct_id is not None:
            related_account_id = AccountId.from_xml(cdtr_acct_id)
        else:
            related_account_id = None

    dbtr_agt_id = ntry.find("NtryDtls/TxDtls/RltdAgts/DbtrAgt/FinInstnId")
    if dbtr_agt_id is not None:
        related_account_bank_id = BankId.from_xml(dbtr_agt_id)
    else:
        cdtr_agt_id = ntry.find("NtryDtls/TxDtls/RltdAgts/CdtrAgt/FinInstnId")
        if cdtr_agt_id is not None:
            related_account_bank_id = BankId.from_xml(cdtr_agt_id)
        else:
            related_account_bank_id = None

    return Transaction(
        entry_ref=entry_ref,
        ref=ref,
        amount=amount,
        currency=currency,
        operation=operation,
        val_date=val_date,
        book_datetime=book_datetime,
        bai_code=bai_code,
        remote_info=remote_info,
        additional_transaction_info=additional_transaction_info,
        related_account_id=related_account_id,
        related_account_bank_id=related_account_bank_id,
    )


def parse_date_isoformat(s: str) -> datetime.date:
    """
    Parse a date string in ISO format and return a datetime.date object.

    Args:
                    s (str): The ISO-formatted date string.

    Returns:
                    datetime.date: The parsed date.

    Raises:
                    RuntimeWarning: If the input string is not a valid ISO-formatted date.
    """
    try:
        return datetime.date.fromisoformat(s)
    except ValueError:
        warnings.warn(f"Invalid isoformat string: {s!r}", RuntimeWarning)
        return datetime.date.fromisoformat(s[:10])


def flatten_dict(d: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
    """
    Flatten a nested dictionary, adding prefixes to keys.

    Args:
                    d (Dict[str, Any]): The input dictionary.
                    prefix (str): The prefix to be added to keys.

    Returns:
                    Dict[str, Any]: The flattened dictionary.
    """
    output = {}
    for k, v in d.items():
        if isinstance(v, dict):
            output.update(flatten_dict(v, prefix=f"{prefix}{k}_"))
        else:
            output[f"{prefix}{k}"] = v
    return output


def parse_xml(input_xml: str) -> List[BankToCustomerStatement]:
    """
    Parse an XML document containing bank statements and return a list of BankToCustomerStatement instances.

    Args:
                    input_xml (str): The XML document as a string.

    Returns:
                    List[BankToCustomerStatement]: List of BankToCustomerStatement instances.
    """
    raw_xml = input_xml.encode("utf-8")

    raw_xml_no_namespace = raw_xml.replace(
        b'xmlns="urn:iso:std:iso:20022:tech:xsd:camt.053.001.02"', b""
    )
    tree = etree.parse(BytesIO(raw_xml_no_namespace))
    root = tree.getroot()

    statements = [
        parse_statement(stmt) for stmt in root.findall(".//BkToCstmrStmt/Stmt")
    ]
    return statements


def parse_statements_to_dataframe(xml_content: str) -> pd.DataFrame:
    """
    Parse XML content containing bank statements and return a DataFrame.

    Args:
        xml_content (str): The XML content as a string.

    Returns:
        pd.DataFrame: The resulting DataFrame containing bank statement data.
    """
    statements = parse_xml(xml_content)
    print(f"statements: {statements}")

    # Check if the statemetns list is emtpy
    if not statements:
        # return an empty DataFrame
        print("No statements found!")
        return pd.DataFrame()

    dataframe = [statement.as_dataframe() for statement in statements]
    result_df = pd.concat(dataframe, ignore_index=True)
    return result_df


def main(input_xml: str) -> pd.DataFrame:
    """
    Main function to parse an XML document and return a DataFrame.

    Args:
        input_xml (str): The XML document as a string.
    Returns:
        pd.DataFrame: The resulting DataFrame containing bank statement data.
    """
    df = parse_statements_to_dataframe(input_xml)
    return df


# Example usage
# input_xml_string = <Example CAMT53 XML file>

# pd.set_option("display.max_columns", None)
# pd.set_option("display.max_rows", None)

# result_df = main(input_xml_string)
# print(result_df)
