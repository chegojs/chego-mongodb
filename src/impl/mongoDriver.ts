import { MongoClient, MongoError, Db } from 'mongodb';
import { pipelines } from './pipelines/pipelines';
import { IDatabaseDriver, IQuery } from '@chego/chego-api';
import { IMongoConfig } from '../api/interfaces';
import { newExecutor } from '@chego/chego-database-boilerplate';


export const chegoMongo = (): IDatabaseDriver => {
    let initialized: boolean = false;
    const mongoConfig: IMongoConfig = { url: null };
    let client: MongoClient;
    const driver: IDatabaseDriver = {
        initialize(config: any): IDatabaseDriver {
            Object.assign(mongoConfig, config);
            initialized = true;
            return driver;
        },
        execute: async (queries: IQuery[]): Promise<any> => new Promise((resolve, reject) => {
            if (!initialized) {
                throw new Error('Driver not initialized');
            }
            const db: Db = mongoConfig.database ? client.db(mongoConfig.database) : client.db();
            return newExecutor()
            .withDBRef(db)
            .withPipelines(pipelines)
            .execute(queries)
            .then(resolve)
            .catch(reject);
        }),
        connect: () :Promise<any> => new Promise((resolve, reject) => {
            const options = mongoConfig.clientOptions ? Object.assign(mongoConfig.clientOptions, { useNewUrlParser: true }) : { useNewUrlParser: true };
            MongoClient.connect(mongoConfig.url, options,
                (error: MongoError, mongoClient: MongoClient) => 
                    (client = mongoClient, (error) ? reject(error) : resolve(true)));
        }),
        disconnect: () :Promise<any> => new Promise((resolve) => (client.close(), resolve(true))),
    }
    return driver;
}
