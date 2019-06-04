import { MongoClientOptions } from 'mongodb';

export interface IMongoConfig {
    url:string;
    database?:string;
    clientOptions?:MongoClientOptions;
}