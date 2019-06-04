# chego-mongodb

This is a Google mongodb driver for Chego library.

## Install
```
npm install --save @chego/chego-mongodb
```

## Usage
All you need to do to ensure that your queries are served by Google mongodb, simply create a new Chego object using the `chegomongodb` and configuration object.

```
const { newChego } = require("@chego/chego");
const { chegoMongodb } = require("@chego/chego-mongodb");
const chego = newChego(chegomongodb, {
    apiKey: "xxxxxxxxxxxxxxxxxxxx",
    authDomain: "some-domain.mongodbapp.com",
    databaseURL: "https://some-domain.mongodbio.com",
    projectId: "some-domain",
    storageBucket: "some-domain.appspot.com",
    messagingSenderId: "3252523423"
});

chego.connect();
const query = newQuery();

query.select('*').from('superheroes').where('origin').is.eq('Gotham City').limit(10);

chego.execute(query)
.then(result => { 
    console.log('RESULT:', JSON.stringify(result));
    chego.disconnect();
})
.catch(error => { 
    console.log('ERROR:', error); 
    chego.disconnect();
});


```
For more information on how `Chego` works with database drivers, please read [Chego Usage guide](https://github.com/chegojs/chego/blob/master/README.md).


## Contribute
There is still a lot to do, so if you want to be part of the Chego project and make it better, it's great.
Whether you find a bug or have a feature request, please contact us. With your help, we'll make it a great tool.

[How to contribute](https://github.com/orgs/chegojs/chego/CONTRIBUTING.md)

Follow our kanban boards to be up to date

[Kanban boards](https://github.com/orgs/chegojs/projects/5)

Join the team, feel free to catch any task or suggest a new one.

## License

Copyright (c) 2019 [Chego Team](https://github.com/orgs/chegojs/people)

Licensed under the [MIT license](LICENSE).