import {
    FunctionData, QuerySyntaxEnum, Fn, Atan2Param, BinData, InsertParam, PadParam, RadiansData, SinData, TanData, FormatData,
    CosData, CotData, DegreesData, RoundData, InsertData, LeftData, LPadData, ReverseData, ReplaceData, RepeatData, SubstrIndexData,
    AsciiData, AsinData, AtanData, Atan2Data, BinaryData, CastAsBinaryData, CastAsDateData, CastAsDatetimeData, CastAsTimeData,
    CastAsSignedData, CastAsUnsignedData
} from '@chego/chego-api';
import { Row, IQueryContext } from '@chego/chego-database-boilerplate';
import { isProperty } from "@chego/chego-tools";

const ctg = (x: number): number => 1 / Math.tan(x);
const degrees = (radians: number): number => radians * (180 / Math.PI);
const radians = (degrees: number): number => degrees * (Math.PI / 180);
const round = (x: number, decimal: number): number => {
    const d: number = Math.pow(100, decimal);
    return Math.round(x * d) / d;
}

const formatNumber = (value: number, decimal: number): string => value.toFixed(decimal).replace(/\d(?=(\d{3})+\.)/g, '$&,');
const useMathFn = (mathFn: (...args: any[]) => number | string, alias: string, param: any, ...rest: any[]) => (rows: Row[], row: Row) => {
    if (isProperty(param)) {
        const value: any = row.content[param.name];
        row.content[alias] = isNumeric(value) ? mathFn(value, ...rest) : 0;
    } else {
        row.content[alias] = isNumeric(param) ? mathFn(param, ...rest) : 0;
    }
    return [...rows, row];
}

const isNumeric = (n: any) => !isNaN(parseFloat(n)) && isFinite(n);

const getAscii = (param: any, alias: string) => (rows: Row[], row: Row) => {
    row.content[alias] = String(isProperty(param) ? row.content[param.name] : param).charCodeAt(0);
    return [...rows, row];
}

const getNumericValue = (row: Row, param: any) => isProperty(param.x)
    ? isNumeric(row.content[param.x.name]) ? row.content[param.x.name] : 0
    : isNumeric(param) ? param : 0;

const getAtan2 = (param: Atan2Param, alias: string) => (rows: Row[], row: Row) => {
    const x: number = getNumericValue(row, param.x);
    const y: number = getNumericValue(row, param.y);
    row.content[alias] = Math.atan2(y, x);

    return [...rows, row];
}
const getBin = (param: any, alias: string) => (rows: Row[], row: Row) => {
    if (isProperty(param)) {
        const value: any = row.content[param.name];
        row.content[alias] = isNumeric(value) ? parseInt(value, 10).toString(2) : 0;
    } else {
        row.content[alias] = isNumeric(param) ? Math.atan(param) : 0;
    }
    return [...rows, row];
}
const parseBin = (rows: Row[], fnData: BinData): Row[] => rows.reduce(getBin(fnData.param, fnData.alias), []);

const getBinary = (param: any, alias: string) => (rows: Row[], row: Row) => {
    row.content[alias] = isProperty(param) ? row.content[param.name] : param;
    return [...rows, row];
}

const insertStrings = (param: InsertParam, alias: string) => (rows: Row[], row: Row) => {
    const base: string = String(isProperty(param.value) ? row.content[param.value.name] : param.value);
    const newString: string = String(isProperty(param.toInsert) ? row.content[param.toInsert.name] : param.toInsert);
    row.content[alias] = base.slice(0, param.position) + newString + base.slice(param.position + Math.abs(length));
    return [...rows, row];
}

const sliceString = (alias: string, value: any, start: number, charsCount: number) => (rows: Row[], row: Row) => {
    const phrase: string = String(isProperty(value) ? row.content[value.name] : value);
    row.content[alias] = phrase.slice(start, charsCount);
    return [...rows, row];
}

const padString = (alias: string, left: boolean, param: PadParam) => (rows: Row[], row: Row) => {
    const value: string = String(isProperty(param.value) ? row.content[param.value.name] : param.value);
    const value2: string = String(isProperty(param.value2) ? row.content[param.value2.name] : param.value2);
    row.content[alias] = left ? value.padStart(param.length, value2) : value.padEnd(param.length, value2);
    return [...rows, row];
}

const repeatString = (alias: string, value: any, count: any) => (rows: Row[], row: Row) => {
    const phrase: string = String(isProperty(value) ? row.content[value.name] : value);
    const size: number = Number(isProperty(count) ? row.content[count.name] : count);
    row.content[alias] = Array<string>(size).fill(phrase).join('');
    return [...rows, row];
}

const replaceString = (alias: string, value: any, from: any, to: any) => (rows: Row[], row: Row) => {
    const base: string = String(isProperty(value) ? row.content[value.name] : value);
    const phrase: string = String(isProperty(from) ? row.content[from.name] : from);
    const replacement: string = String(isProperty(to) ? row.content[to.name] : to);
    row.content[alias] = base.replace(new RegExp(phrase, 'g'), replacement);
    return [...rows, row];
}

const reverseString = (alias: string, value: any) => (rows: Row[], row: Row) => {
    const phrase: string = String(isProperty(value) ? row.content[value.name] : value);
    row.content[alias] = phrase.split('').reverse().join('');
    return [...rows, row];
}

const getSubstringBeforeDelimiter = (alias: string, value: any, delimiter: string, count: number) => (rows: Row[], row: Row) => {
    const base: string = String(isProperty(value) ? row.content[value.name] : value);
    row.content[alias] = base.split(delimiter, count).join(delimiter).length;
    return [...rows, row];
}

const parseInteger = (unsigned: boolean, value: number) => isNumeric(value) && ((unsigned && value > 0) || !unsigned) ? Math.round(value) : 0;

const castInteger = (unsigned: boolean, param: any, alias: string) => (rows: Row[], row: Row) => {
    row.content[alias] = parseInteger(unsigned, isProperty(param) ? Number(row.content[param.name]) : Number(param));
    return [...rows, row];
}

const daysInMonth = (month: number, year: number) => {
    switch (month) {
        case 2:
            return (year % 4 === 0 && year % 100) || year % 400 === 0 ? 29 : 28;
        case 4:
        case 6:
        case 9:
        case 11:
            return 30;
        default:
            return 31
    }
}

const isValidDate = (day: number, month: number, year: number) =>
    month >= 0 && month < 12 && day > 0 && day <= daysInMonth(month, year);

const parseYears = (value: string) => {
    const yearsNr = Number(value);
    if (yearsNr < 0 || yearsNr > 9999) {
        return null;
    } else if (value.length === 1) {
        return (yearsNr === 0) ? `0000` : `200${value}`;
    } else if (value.length === 2) {
        return (yearsNr > 69) ? `19${value}` : `20${value}`;
    } else if (value.length === 3) {
        return `0${value}`;
    } else {
        return value;
    }
}

const parseMonths = (value: string) => {
    const months = Number(value);
    return (months >= 0 || months <= 12)
        ? (months < 10)
            ? `0${months}`
            : value
        : null;
}

const parseDays = (value: string, month: number, year: number) => {
    const day = Number(value);
    if (isValidDate(day, month, year)) {
        return (day >= 0 || day <= 31)
            ? (day < 10)
                ? `0${day}`
                : value
            : null
    }
    return null;
}

const parseDate = (date: string[]): string => {
    if (date) {
        const dateParts: string[] = date[0].split(/\D+/);
        const year: string = parseYears(dateParts[0]);
        const month: string = parseMonths(dateParts[1]);
        const day: string = parseDays(dateParts[2], Number(month), Number(year));
        return (year && month && day) ? `${year}-${month}-${day}` : null;
    }
    return null;
}

const parseTimeElement = (value: string, minValue: number, maxValue: number): string => {
    const i: number = Number(value);
    return i
        ? (i >= minValue && i <= maxValue)
            ? (i < 10) ? `0${i}` : value
            : null
        : '00';
}

const parseTime = (time: string[]): string => {
    if (time) {
        const timeParts: string[] = time[0].split(/\D+/);
        const hh: string = parseTimeElement(timeParts[0], 0, 23);
        const mm: string = parseTimeElement(timeParts[1], 0, 59);
        const ss: string = parseTimeElement(timeParts[2], 0, 59);
        return (hh && mm && ss) ? `${hh}:${mm}:${ss}` : null;
    }
    return null;
}

const castDate = (param: any, alias: string, type: CastDateType) => (rows: Row[], row: Row) => {
    const value = isProperty(param) ? row.content[param.name] : param;
    const dateMatch: string[] = value.match(/\d+[^A-Za-z0-9\s\:]+\d+[^A-Za-z0-9\s\:]+\d+/);
    const timeMatch: string[] = value.match(/\d{2}\:\d{2}\:\d{2}/);

    if (type === CastDateType.Date) {
        row.content[alias] = parseDate(dateMatch);
    } else if (type === CastDateType.Time) {
        row.content[alias] = parseTime(timeMatch);
    } else {
        const date: string = parseDate(dateMatch);
        const time: string = parseTime(timeMatch);
        row.content[alias] = (date && time) ? `${date} ${time}` : null;
    }

    return [...rows, row];
}

const parseRadians = (rows: Row[], fnData: RadiansData): Row[] =>
    rows.reduce(useMathFn(radians, fnData.alias, fnData.param), []);

const parseSin = (rows: Row[], fnData: SinData): Row[] =>
    rows.reduce(useMathFn(Math.sin, fnData.alias, fnData.param), []);

const parseTan = (rows: Row[], fnData: TanData): Row[] =>
    rows.reduce(useMathFn(Math.tan, fnData.alias, fnData.param), []);

const parseFormat = (rows: Row[], fnData: FormatData): Row[] =>
    rows.reduce(useMathFn(formatNumber, fnData.alias, fnData.param.value, fnData.param.decimal), []);

const parseCos = (rows: Row[], fnData: CosData): Row[] =>
    rows.reduce(useMathFn(Math.cos, fnData.alias, fnData.param), []);

const parseCot = (rows: Row[], fnData: CotData): Row[] =>
    rows.reduce(useMathFn(ctg, fnData.alias, fnData.param), []);

const parseDegrees = (rows: Row[], fnData: DegreesData): Row[] =>
    rows.reduce(useMathFn(degrees, fnData.alias, fnData.param), []);

const parseRound = (rows: Row[], fnData: RoundData): Row[] =>
    rows.reduce(useMathFn(round, fnData.alias, fnData.param.value, fnData.param.decimal), []);

const parseInsertString = (rows: Row[], fnData: InsertData): Row[] =>
    rows.reduce(insertStrings(fnData.param, fnData.alias), []);

const parseRight = (rows: Row[], fnData: LeftData): Row[] =>
    rows.reduce(sliceString(fnData.alias, fnData.param.value, -1, fnData.param.charsCount), []);

const parseLeft = (rows: Row[], fnData: LeftData): Row[] =>
    rows.reduce(sliceString(fnData.alias, fnData.param.value, 0, fnData.param.charsCount), []);

const parseRPad = (rows: Row[], fnData: LPadData): Row[] =>
    rows.reduce(padString(fnData.alias, false, fnData.param), []);

const parseLPad = (rows: Row[], fnData: LPadData): Row[] =>
    rows.reduce(padString(fnData.alias, true, fnData.param), []);

const parseReverse = (rows: Row[], fnData: ReverseData): Row[] =>
    rows.reduce(reverseString(fnData.alias, fnData.param), []);

const parseReplace = (rows: Row[], fnData: ReplaceData): Row[] =>
    rows.reduce(replaceString(fnData.alias, fnData.param.value, fnData.param.from, fnData.param.to), []);

const parseRepeat = (rows: Row[], fnData: RepeatData): Row[] =>
    rows.reduce(repeatString(fnData.alias, fnData.param.value, fnData.param.count), []);

const parseSubstringIndex = (rows: Row[], fnData: SubstrIndexData): Row[] =>
    rows.reduce(getSubstringBeforeDelimiter(fnData.alias, fnData.param.value, fnData.param.delimiter, fnData.param.count), []);

const parseAscii = (rows: Row[], fnData: AsciiData): Row[] =>
    rows.reduce(getAscii(fnData.param, fnData.alias), []);

const parseAsin = (rows: Row[], fnData: AsinData): Row[] =>
    rows.reduce(useMathFn(Math.asin, fnData.alias, fnData.param), []);

const parseAtan = (rows: Row[], fnData: AtanData): Row[] =>
    rows.reduce(useMathFn(Math.atan, fnData.alias, fnData.param), []);

const parseAtan2 = (rows: Row[], fnData: Atan2Data): Row[] =>
    rows.reduce(getAtan2(fnData.param, fnData.alias), []);
// mock
const parseBinary = (rows: Row[], fnData: BinaryData): Row[] =>
    rows.reduce(getBinary(fnData.param, fnData.alias), []);
// mock
const parseCastAsBinary = (rows: Row[], fnData: CastAsBinaryData): Row[] =>
    rows.reduce(getBinary(fnData.param, fnData.alias), []);
// mock
const parseCastAsChar = (rows: Row[], fnData: CastAsBinaryData): Row[] =>
    rows.reduce(getBinary(fnData.param, fnData.alias), []);

const parseCastAsDate = (rows: Row[], fnData: CastAsDateData): Row[] =>
    rows.reduce(castDate(fnData.param, fnData.alias, CastDateType.Date), []);

const parseCastAsDatetime = (rows: Row[], fnData: CastAsDatetimeData): Row[] =>
    rows.reduce(castDate(fnData.param, fnData.alias, CastDateType.Datetime), []);

const parseCastAsTime = (rows: Row[], fnData: CastAsTimeData): Row[] =>
    rows.reduce(castDate(fnData.param, fnData.alias, CastDateType.Time), []);

const parseCastAsSigned = (rows: Row[], fnData: CastAsSignedData): Row[] =>
    rows.reduce(castInteger(false, fnData.param, fnData.alias), []);

const parseCastAsUnsigned = (rows: Row[], fnData: CastAsUnsignedData): Row[] =>
    rows.reduce(castInteger(true, fnData.param, fnData.alias), []);

const mysqlFunctions: Map<QuerySyntaxEnum, Fn<Row[]>> = new Map<QuerySyntaxEnum, Fn<Row[]>>([
    [QuerySyntaxEnum.Ascii, parseAscii],
    [QuerySyntaxEnum.Asin, parseAsin],
    [QuerySyntaxEnum.Atan, parseAtan],
    [QuerySyntaxEnum.Atan2, parseAtan2],
    [QuerySyntaxEnum.Bin, parseBin],
    [QuerySyntaxEnum.Binary, parseBinary],
    [QuerySyntaxEnum.CastAsBinary, parseCastAsBinary],
    [QuerySyntaxEnum.CastAsChar, parseCastAsChar],
    [QuerySyntaxEnum.CastAsDate, parseCastAsDate],
    [QuerySyntaxEnum.CastAsDatetime, parseCastAsDatetime],
    [QuerySyntaxEnum.CastAsTime, parseCastAsTime],
    [QuerySyntaxEnum.CastAsSigned, parseCastAsSigned],
    [QuerySyntaxEnum.CastAsUnsigned, parseCastAsUnsigned],
    [QuerySyntaxEnum.Cos, parseCos],
    [QuerySyntaxEnum.Cot, parseCot],
    [QuerySyntaxEnum.Degrees, parseDegrees],
    [QuerySyntaxEnum.Format, parseFormat],
    [QuerySyntaxEnum.InsertString, parseInsertString],
    [QuerySyntaxEnum.Left, parseLeft],
    [QuerySyntaxEnum.Radians, parseRadians],
    [QuerySyntaxEnum.Repeat, parseRepeat],
    [QuerySyntaxEnum.ReplaceString, parseReplace],
    [QuerySyntaxEnum.Reverse, parseReverse],
    [QuerySyntaxEnum.Right, parseRight],
    [QuerySyntaxEnum.Round, parseRound],
    [QuerySyntaxEnum.Rpad, parseRPad],
    [QuerySyntaxEnum.Lpad, parseLPad],
    [QuerySyntaxEnum.Sin, parseSin],
    [QuerySyntaxEnum.SubstringIndex, parseSubstringIndex],
    [QuerySyntaxEnum.Tan, parseTan],
]);

const applyMySQLFunctions = (sourceRows: Row[]) => (parsedRows:Row[], fnData: FunctionData) => {
    const mySQLFn: Fn<Row[]> = mysqlFunctions.get(fnData.type);
    if (mySQLFn) {
        const fnResults: Row[] = mySQLFn(sourceRows, fnData);
        parsedRows.push(...fnResults);
    }
    return parsedRows;
}

export const applyMySQLFunctionsIfAny = (queryContext: IQueryContext) => (rows: Row[]): Row[] => 
    (queryContext.functions.length) 
        ? queryContext.functions.reduce(applyMySQLFunctions(rows), [])
        : rows;