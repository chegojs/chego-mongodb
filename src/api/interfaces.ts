import { MongoClientOptions } from 'mongodb';
import { IQueryContext } from '@chego/chego-nosql';
import { Table } from '@chego/chego-api';

export interface IMongoConfig {
    url:string;
    database?:string;
    clientOptions?:MongoClientOptions;
}

export interface IPipelineBuilder {
    with(queryContext:IQueryContext, defaultTable:Table): IPipelineBuilder;
    build(): object[]
}