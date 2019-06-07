import { MongoClientOptions } from 'mongodb';
import { Table } from '@chego/chego-api';
import { IQueryContext } from '@chego/chego-database-boilerplate';

export interface IMongoConfig {
    url:string;
    database?:string;
    clientOptions?:MongoClientOptions;
}

export interface IPipelineBuilder {
    with(queryContext:IQueryContext, defaultTable:Table): IPipelineBuilder;
    build(): object[]
}