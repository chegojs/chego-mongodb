import { Db } from 'mongodb';
import { IQueryContext } from '@chego/chego-nosql';
import { Table } from '@chego/chego-api';

export const insertData = (ref: Db, table: Table, data:any) => async (): Promise<any> => new Promise((resolve, reject) =>
    ref.collection(table.name).insertMany(data).then(resolve).catch(reject));

const chainInsertTasks = (ref: Db, data:any) => (previousTask:Promise<any>, table:Table): Promise<any> => 
    previousTask.then(insertData(ref, table, data));

export const runInsertPipeline = async (ref: Db, queryContext: IQueryContext): Promise<any> =>
    new Promise(async (resolve, reject) => 
        queryContext.tables.reduce(chainInsertTasks(ref, queryContext.data), Promise.resolve())
        .then(resolve)
        .catch(reject));
