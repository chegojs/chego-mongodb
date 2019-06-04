import { IQueryContext } from '@chego/chego-nosql';
import { Db } from 'mongodb';
import { Table } from '@chego/chego-api';
import { buildConditions } from './select';

export const removeDocuments = (ref: Db, table: Table, options: object) => async (): Promise<any> =>
    new Promise((resolve, reject) =>
        ref.collection(table.name).deleteMany(options).then(resolve).catch(reject));

const chainRemoveTasks = (ref: Db, options: object) => (previousTask: Promise<any>, table: Table) =>
    previousTask.then(removeDocuments(ref, table, options));

export const runDeletePipeline = async (ref: Db, queryContext: IQueryContext): Promise<any> =>
    new Promise(async (resolve, reject) => {
        const filter: object = queryContext.conditions.reduce(buildConditions, {});
        return queryContext.tables.reduce(chainRemoveTasks(ref, filter),
            Promise.resolve()).then(resolve).catch(reject);
    });