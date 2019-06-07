import { Obj } from '@chego/chego-api';

const mergeObjectsRecursively = (target:Obj, current:Obj) => {
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

export const mergeObjects = (target: Obj, ...objs: Obj[]) => objs.reduce(mergeObjectsRecursively, target);