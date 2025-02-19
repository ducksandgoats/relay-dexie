import {Dexie} from 'dexie'
import "dexie-export-import"

export default class Base {
    constructor(opts){
        this._debug = opts.debug

        if(!opts.proto){
            throw new Error('must have proto')
        }

        if(opts.proto !== 'msg:' && opts.proto !== 'topic:' && opts.proto !== 'pubsub:'){
            throw new Error('proto must be msg:, topic:, or pubsub:')
        }

        this._proto = opts.proto

        if(!opts.id){
            throw new Error('must have id')
        }

        this._id = opts.id

        this._load = typeof(opts.load) === 'object' && !Array.isArray(opts.load) ? opts.load : {from: Date.now() - 86400000}

        this._span = localStorage.getItem('save') ? Number(localStorage.getItem('save')) : null

        this._sync = Boolean(opts.sync)

        this._force = opts.force === false ? opts.force : true
    
        this._keep = opts.keep === true ? opts.keep : false

        this._timer = typeof(opts.timer) === 'object' && !Array.isArray(opts.timer) ? opts.timer : {}
        this._timer.redo = this._timer.redo || 180000
        this._timer.expire = this._timer.expire || 300000
        this._timer.save = this._timer.save || 60000
    
        this._user = localStorage.getItem('user') || (() => {const test = crypto.randomUUID();localStorage.setItem('user', test);return test;})()
    
        opts.own = typeof(opts.own) === 'object' && !Array.isArray(opts.own) ? opts.own : {}
        this.checkForOwnTables = Object.keys(opts.own)
        for(const records in opts.schema){
            const record = opts.schema[records].split(',').map((data) => {return data.replaceAll(' ', '')})
            if(!record.includes('stamp')){
                record.push('stamp')
            }
            if(!record.includes('edit')){
                record.push('edit')
            }
            if(!record.includes('user')){
                record.push('user')
            }
            if(record.includes('iden')){
                record.splice(record.indexOf('iden'), 1)
                record.unshift('iden')
            } else {
                record.unshift('iden')
            }
            opts.schema[records] = record.join(',')
        }
        
        this.db = new Dexie(opts.name, {})
        if(this._debug){
            console.log('name', this.db.name)
        }
        this.db.version(opts.version).stores({...opts.own, ...opts.schema})

        if(opts.routine){
            this._routine = setInterval(() => {
                this.initUser().then(console.log).catch(console.error)
            }, this._timer.redo)
        }
        this._save = setInterval(() => {
            localStorage.setItem('save', `${Date.now()}`)
        }, this._timer.save)

        this._piecing = new Map()
        ;(async () => {
            for await (const i of (await fetch(`${this._proto}//${this._id}/`, {method: 'GET'})).body){
                await this.handler(i)
            }
        })().then((data) => {console.log(data)}).catch((err) => {console.error(err.name, err.message, err.stack)});
    }

    async initUser(){
        const useHeaders = (await fetch(`${this._proto}//${this._id}`, {method: 'HEAD', headers: {'X-Iden': 'true', 'X-Buf': 'false'}})).headers
        const iden = useHeaders.has('X-Iden') ? useHeaders.get('X-Iden') : null
        if(iden){
            for(const table of this.db.tables){
                if(this.checkForOwnTables.includes(table.name)){
                    continue
                }
                if(this._sync === true){
                    await fetch(`${this._proto}//${this._id}/`, {method: 'POST', headers: {'X-Iden': iden}, body: JSON.stringify({name: table.name, session: 'stamp'})})
                    await fetch(`${this._proto}//${this._id}/`, {method: 'POST', headers: {'X-Iden': iden}, body: JSON.stringify({name: table.name, session: 'edit'})})
                } else if(this._sync === null){
                    if(this._span){
                        await fetch(`${this._proto}//${this._id}/`, {method: 'POST', headers: {'X-Iden': iden}, body: JSON.stringify({between: {from: this._span, to: Date.now()}, name: table.name, session: 'stamp'})})
                        await fetch(`${this._proto}//${this._id}/`, {method: 'POST', headers: {'X-Iden': iden}, body: JSON.stringify({between: {from: this._span, to: Date.now()}, name: table.name, session: 'edit'})})
                    } else {
                        await fetch(`${this._proto}//${this._id}/`, {method: 'POST', headers: {'X-Iden': iden}, body: JSON.stringify({...this._load, name: table.name, session: 'stamp'})})
                        await fetch(`${this._proto}//${this._id}/`, {method: 'POST', headers: {'X-Iden': iden}, body: JSON.stringify({...this._load, name: table.name, session: 'edit'})})
                    }
                } else if(this._sync === false){
                    const s = await table.where('stamp').notEqual(0).last()
                    const e = await table.where('edit').notEqual(0).last()
                    if(s){
                        await fetch(`${this._proto}//${this._id}/`, {method: 'POST', headers: {'X-Iden': iden}, body: JSON.stringify({from: s.stamp - 300000, name: table.name, session: 'stamp'})})
                    }
                    if(e){
                        await fetch(`${this._proto}//${this._id}/`, {method: 'POST', headers: {'X-Iden': iden}, body: JSON.stringify({from: e.edit - 300000, name: table.name, session: 'edit'})})
                    }
                } else {
                    continue
                }
            }
        }
    }

    async handler(data, nick){
        try {
            if(this._debug){
                console.log('Received Message: ', typeof(data), data, new TextDecoder().decode(data))
            }

            const datas = JSON.parse(new TextDecoder().decode(data))

            const dataTab = this.db.table(datas.name)

            if(datas.status){
                if(datas.user === this._user){
                    return
                }
                if(datas.status === 'add'){
                    await dataTab.add(datas.data)
                } else if(datas.status === 'edit'){
                    await dataTab.update(datas.iden, datas.data)
                } else if(datas.status === 'sub'){
                    if(!this._keep){
                        await dataTab.delete(datas.iden)
                    }
                } else {
                    return
                }
            } else if(datas.session){
                if(datas.session === 'stamp'){
                    let stamps
                    if(datas.between){
                        if(!datas.includes){
                            datas.includes = {from: true, to: true}
                        }
                        stamps = await dataTab.where('stamp').between(datas.between.from, datas.between.to, datas.includes.from, datas.includes.to).toArray()
                    } else if(datas.from){
                        stamps = await dataTab.where('stamp').above(datas.from).toArray()
                    } else if(datas.to){
                        stamps = await dataTab.where('stamp').below(datas.to).toArray()
                    } else {
                        stamps = await dataTab.where('stamp').notEqual(0).toArray()
                    }
                    const count = datas.count || 15
                    while(stamps.length){
                        datas.session = 'stamps'
                        datas.stamps = stamps.splice(stamps.length - count, count)
                        datas.edits = null
                        const test = JSON.stringify(datas)
                        if(test.length < 16000){
                            await fetch(`${this._proto}//${this._id}/`, {method: 'POST', headers: {'X-Iden': nick}, body: test})
                        } else {
                            const useID = crypto.randomUUID()
                            const pieces = Math.ceil(test.length / 15000)
                            let used = 0
                            for(let i = 1;i < (pieces + 1);i++){
                                const slicing = i * 15000
                                await fetch(`${this._proto}//${this._id}/`, {method: 'POST', headers: {'X-Iden': nick}, body: JSON.stringify({name: datas.name, piecing: 'stamps', pieces, piece: i, iden: useID, stamps: test.slice(used, slicing)})})
                                used = slicing
                            }
                        }
                    }
                } else if(datas.session === 'stamps'){
                    if(!datas.stamps.length){
                        return
                    }
                    for(const data of datas.stamps){
                        try {
                            await dataTab.add(data)
                        } catch (err) {
                            if(this._debug){
                                console.error(err)
                            }
                            try {
                                const got = await dataTab.get(data.iden)
                                if(got){
                                    if(got.edit < data.edit){
                                        await dataTab.put(data)
                                    }
                                }
                            } catch (error) {
                                if(this._debug){
                                    console.error(error)
                                }
                            }
                        }
                    }
                } else if(datas.session === 'edit'){
                    let edits
                    if(datas.between){
                        if(!datas.includes){
                            datas.includes = {from: true, to: true}
                        }
                        edits = await dataTab.where('edit').between(datas.between.from, datas.between.to, datas.includes.from, datas.includes.to).toArray()
                    } else if(datas.from){
                        edits = await dataTab.where('edit').above(datas.from).toArray()
                    } else if(datas.to){
                        edits = await dataTab.where('edit').below(datas.to).toArray()
                    } else {
                        edits = await dataTab.where('edit').notEqual(0).toArray()
                    }
                    const count = datas.count || 15
                    while(edits.length){
                        datas.session = 'edits'
                        datas.stamps = null
                        datas.edits = edits.splice(edits.length - count, count)
                        const test = JSON.stringify(datas)
                        if(test.length < 16000){
                            await fetch(`${this._proto}//${this._id}/`, {method: 'POST', headers: {'X-Iden': nick}, body: test})
                        } else {
                            const useID = crypto.randomUUID()
                            const pieces = Math.ceil(test.length / 15000)
                            let used = 0
                            for(let i = 1;i < (pieces + 1);i++){
                                const slicing = i * 15000
                                await fetch(`${this._proto}//${this._id}/`, {method: 'POST', headers: {'X-Iden': nick}, body: JSON.stringify({name: datas.name, piecing: 'edits', pieces, piece: i, iden: useID, edits: test.slice(used, slicing)})})
                                used = slicing
                            }
                        }
                    }
                } else if(datas.session === 'edits'){
                    if(!datas.edits.length){
                        return
                    }
                    for(const data of datas.edits){
                        try {
                            await dataTab.add(data)
                        } catch (err) {
                            if(this._debug){
                                console.error(err)
                            }
                            try {
                                const got = await dataTab.get(data.iden)
                                if(got){
                                    if(got.edit < data.edit){
                                        await dataTab.put(data)
                                    }
                                }
                            } catch (error) {
                                if(this._debug){
                                    console.error(error)
                                }
                            }
                        }
                    }
                } else {
                    return
                }
            } else if(datas.piecing){
                if(datas.piecing === 'add'){
                    if(this._piecing.has(datas.iden)){
                        const obj = this._piecing.get(datas.iden)
                        if(!obj.arr[datas.piece - 1]){
                            obj.arr[datas.piece - 1] = datas.data
                            obj.stamp = Date.now()
                            if(obj.arr.every(Boolean)){
                                const useData = JSON.parse(obj.arr.join(''))
                                await dataTab.add(useData.data)
                                this._piecing.delete(datas.iden)
                            }
                        }
                    } else {
                        const obj = {stamp: Date.now(), arr: new Array(datas.pieces).fill(null)}
                        this._piecing.set(datas.iden, obj)
                        if(!obj.arr[datas.piece - 1]){
                            obj.arr[datas.piece - 1] = datas.data
                            obj.stamp = Date.now()
                        }
                    }
                } else if(datas.piecing === 'edit'){
                    if(this._piecing.has(datas.iden)){
                        const obj = this._piecing.get(datas.iden)
                        if(!obj.arr[datas.piece - 1]){
                            obj.arr[datas.piece - 1] = datas.data
                            obj.stamp = Date.now()
                            if(obj.arr.every(Boolean)){
                                const useData = JSON.parse(obj.arr.join(''))
                                await dataTab.update(datas.iden, useData.data)
                                this._piecing.delete(datas.iden)
                            }
                        }
                    } else {
                        const obj = {stamp: Date.now(), arr: new Array(datas.pieces).fill(null)}
                        this._piecing.set(datas.iden, obj)
                        if(!obj.arr[datas.piece - 1]){
                            obj.arr[datas.piece - 1] = datas.data
                            obj.stamp = Date.now()
                        }
                    }
                } else if(datas.piecing === 'stamps'){
                    if(this._piecing.has(datas.iden)){
                        const obj = this._piecing.get(datas.iden)
                        if(!obj.arr[datas.piece - 1]){
                            obj.arr[datas.piece - 1] = datas.data
                            obj.stamp = Date.now()
                            if(obj.arr.every(Boolean)){
                                const useData = JSON.parse(obj.arr.join(''))
                                if(!useData.stamps.length){
                                    return
                                }
                                for(const data of useData.stamps){
                                    try {
                                        await dataTab.add(data)
                                    } catch (err) {
                                        if(this._debug){
                                            console.error(err)
                                        }
                                        try {
                                            const got = await dataTab.get(data.iden)
                                            if(got){
                                                if(got.edit < data.edit){
                                                    await dataTab.put(data)
                                                }
                                            }
                                        } catch (error) {
                                            if(this._debug){
                                                console.error(error)
                                            }
                                        }
                                    }
                                }
                                this._piecing.delete(datas.iden)
                            }
                        }
                    } else {
                        const obj = {stamp: Date.now(), arr: new Array(datas.pieces).fill(null)}
                        this._piecing.set(datas.iden, obj)
                        if(!obj.arr[datas.piece - 1]){
                            obj.arr[datas.piece - 1] = datas.data
                            obj.stamp = Date.now()
                        }
                    }
                } else if(datas.piecing === 'edits'){
                    if(this._piecing.has(datas.iden)){
                        const obj = this._piecing.get(datas.iden)
                        if(!obj.arr[datas.piece - 1]){
                            obj.arr[datas.piece - 1] = datas.data
                            obj.stamp = Date.now()
                            if(obj.arr.every(Boolean)){
                                const useData = JSON.parse(obj.arr.join(''))
                                if(!useData.edits.length){
                                    return
                                }
                                for(const data of useData.edits){
                                    try {
                                        await dataTab.add(data)
                                    } catch (err) {
                                        if(this._debug){
                                            console.error(err)
                                        }
                                        try {
                                            const got = await dataTab.get(data.iden)
                                            if(got){
                                                if(got.edit < data.edit){
                                                    await dataTab.put(data)
                                                }
                                            }
                                        } catch (error) {
                                            if(this._debug){
                                                console.error(error)
                                            }
                                        }
                                    }
                                }
                                this._piecing.delete(datas.iden)
                            }
                        }
                    } else {
                        const obj = {stamp: Date.now(), arr: new Array(datas.pieces).fill(null)}
                        this._piecing.set(datas.iden, obj)
                        if(!obj.arr[datas.piece - 1]){
                            obj.arr[datas.piece - 1] = datas.data
                            obj.stamp = Date.now()
                        }
                    }
                } else {
                    return
                }
            } else {
                return
            }
        } catch (err) {
            if(this._debug){
                console.error(err)
            }
            return
        }
    }

    id(){return crypto.randomUUID()}

    async ret(name, prop){
        const dataTab = this.db.table(name)
        return await dataTab.get(prop)
    }

    async add(name, data){
        const dataTab = this.db.table(name)
        data.stamp = data.stamp || Date.now()
        data.user = data.user || this._user
        data.iden = data.iden || crypto.randomUUID()
        data.edit = 0
        const test = await dataTab.add(data)
        const useData = JSON.stringify({name, data, user: data.user, stamp: data.stamp, iden: test, status: 'add'})
        if(useData.length < 16000){
            await fetch(`${this._proto}//${this._id}/`, {method: 'POST', headers: {}, body: JSON.stringify(useData)})
        } else {
            const pieces = Math.ceil(useData.length / 15000)
            let used = 0
            for(let i = 1;i < (pieces + 1);i++){
                const slicing = i * 15000
                await fetch(`${this._proto}//${this._id}/`, {method: 'POST', headers: {}, body: JSON.stringify({name, data: useData.slice(used, slicing), user: data.user, stamp: data.stamp, iden: test, piecing: 'add', pieces, pice: i})})
                used = slicing
            }
        }
        return test
    }

    async edit(name, prop, data){
        const dataTab = this.db.table(name)
        const test = await dataTab.get(prop)
        if((test && test.user === this._user) && (!data.user || data.user === this._user)){
            data.edit = Date.now()
            const num = await dataTab.update(prop, data)
            const useData = JSON.stringify({name, data, iden: test.iden, user: test.user, edit: data.edit, num, status: 'edit'})
            if(useData.length < 16000){
                await fetch(`${this._proto}//${this._id}/`, {method: 'POST', headers: {}, body: JSON.stringify(useData)})
            } else {
                const pieces = Math.ceil(useData.length / 15000)
                let used = 0
                for(let i = 1;i < (pieces + 1);i++){
                    const slicing = i * 15000
                    await fetch(`${this._proto}//${this._id}/`, {method: 'POST', headers: {}, body: JSON.stringify({name, data: useData.slice(used, slicing), iden: test.iden, user: test.user, edit: data.edit, num, piecing: 'edit', pieces, piece: i})})
                    used = slicing
                }
            }
            return test.iden
        } else {
            throw new Error('user does not match')
        }
    }

    async sub(name, prop){
        const dataTab = this.db.table(name)
        const test = await dataTab.get(prop)
        if(!test){
            throw new Error('did not find data')
        }
        if(this._force){
            await dataTab.delete(test.iden)
            if(test.user === this._user){
                await fetch(`${this._proto}//${this._id}/`, {method: 'POST', headers: {'X-Iden': iden}, body: JSON.stringify({name, iden: test.iden, user: test.user, status: 'sub'})})
            }
            return test.iden
        } else {
            if(test.user === this._user){
                await dataTab.delete(test.iden)
                await fetch(`${this._proto}//${this._id}/`, {method: 'POST', headers: {'X-Iden': iden}, body: JSON.stringify({name, iden: test.iden, user: test.user, status: 'sub'})})
                return test.iden
            } else {
                throw new Error('user does not match')
            }
        }
    }

    async clear(name){
        const dataTab = this.db.table(name)
        await dataTab.clear()
    }

    table(name){
        return this.db.table(name)
    }

    async load(name, session, data = {}){
        const dataTab = this.db.table(name)
        data.name = dataTab.name
        data.session = session
        await fetch(`${this._proto}//${this._id}/`, {method: 'POST', headers: {'X-Iden': iden}, body: JSON.stringify(data)})
    }

    async getDB(blob, opts){
        return await this.db.import(blob, opts)
    }

    async postDB(opts){
        return await this.db.export(opts)
    }

    quit(){
        if(this._routine){
            clearInterval(this._routine)
        }
        clearInterval(this._save)
        this.db.close()
    }
}