type ISObject = { [key: string]: any };

const mergeObjectsRecursively = (target:ISObject, current:ISObject) => {
    for (const property in current) {
        try {
            if (current[property].constructor === Object) {
                target[property] = mergeObjects(target[property], current[property]);
            } else {
                target[property] = current[property];
            }
        } catch (e) {
            target[property] = current[property];
        }
    }
    return target;
}

export const mergeObjects = (target: ISObject, ...objs: ISObject[]) => objs.reduce(mergeObjectsRecursively, target);