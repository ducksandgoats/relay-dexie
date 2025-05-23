import {Dexie} from 'dexie'
import {arr2text} from 'uint8-util'
// import "dexie-export-import"

export default class Base {
    constructor(opts){
        this._debug = opts.debug

        if(!opts.proto){
            throw new Error('must have proto')
        }

        if(opts.proto !== 'msg:' && opts.proto !== 'topic:' && opts.proto !== 'pubsub:'){
            throw new Error('proto must be msg:, topic:, or pubsub:')
        }

        opts.init = Boolean(opts.init)

        opts.routine = Boolean(opts.routine)

        this._count = opts.count || 15

        this._proto = opts.proto

        this._ben = this._proto === 'msg:' ? opts.ben && ['str', 'json', 'buf'].includes(opts.ben) ? opts.ben : undefined : undefined

        this._objHeader = this._ben ? {'X-Ben': this._ben} : {}

        if(!opts.id){
            throw new Error('must have id')
        }

        this._users = new Set()

        this._prog = new Set()

        this._id = opts.id

        this._sync = Boolean(opts.sync)

        this._timer = opts.timer || 180000
    
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
            this._routine = setInterval(() => {this.initUser().then(console.log).catch(console.error)}, this._timer)
        }

        this._piecing = new Map()

        if(opts.init){
            this.initUser().then(console.log).catch(console.error)
        }

        ;(async () => {
            for await (const i of (await fetch(`${this._proto}//${this._id}/`, {method: 'GET'})).body){
                await this.handler(i)
            }
        })().then((data) => {console.log(data)}).catch((err) => {console.error(err.name, err.message, err.stack)});
    }

    async initUser(){
        const idens = await (await fetch(`${this._proto}//${this._id}`, {method: 'GET', headers: {'X-Iden': 'true', 'X-Buf': 'false'}})).json()
        for(const iden of idens){
            for(const table of this.db.tables){
                if(this.checkForOwnTables.includes(table.name)){
                    continue
                }
                if(this._users.has(iden)){
                    const s = (await table.where('user').equals(this._user).sortBy('stamp').last())?.stamp
                    await fetch(`${this._proto}//${this._id}/`, {method: 'POST', headers: {'X-Iden': iden, ...this._objHeader}, body: JSON.stringify({sync: this._sync, records: s || 0, name: table.name, session: 'sync', count: this._count})})
                } else {
                    await fetch(`${this._proto}//${this._id}/`, {method: 'POST', headers: {'X-Iden': iden, ...this._objHeader}, body: JSON.stringify({name: table.name, session: 'sync', records: null, sync: this._sync, count: this._count})})
                    this._users.add(iden)
                }
            }
        }
    }

    async handler(data){
        try {

            const arrText = arr2text(data)

            if(this._debug){
                console.log('Received Message: ', typeof(data), data, arrText)
            }

            const {data: datas, nick} = JSON.parse(arrText)

            if(this._debug){
                console.log(datas)
            }

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
                    await dataTab.delete(datas.iden)
                } else {
                    return
                }
            } else if(datas.session){
                if(this._debug){
                    console.log('run session')
                }
                if(datas.session === 'sync'){
                    if(this._debug){
                        console.log('run sync')
                    }

                    let records
                    const useRecords = datas.records ? datas.records - 1 : 0
                    if(datas.sync){
                        if(useRecords){
                            records = await dataTab.where('stamp').above(useRecords).toArray()
                        } else {
                            records = await dataTab.where('stamp').notEqual(0).toArray()
                        }
                    } else {
                        if(useRecords){
                            records = await dataTab.where('user').equals(this._user).filter((blurb) => blurb.stamp > useRecords).toArray()
                        } else {
                            records = await dataTab.where('user').equals(this._user).toArray()
                        }
                    }

                    await fetch(`${this._proto}//${this._id}/`, {method: 'POST', headers: {'X-Iden': nick, ...this._objHeader}, body: JSON.stringify({session: 'start'})})
                    const count = datas.count || 15
                    while(records.length){
                        datas.session = 'records'
                        datas.records = records.splice(records.length - count, count)
                        const test = JSON.stringify(datas)
                        if(test.length < 16000){
                            await fetch(`${this._proto}//${this._id}/`, {method: 'POST', headers: {'X-Iden': nick, ...this._objHeader}, body: test})
                        } else {
                            const useID = crypto.randomUUID()
                            const pieces = Math.ceil(test.length / 15000)
                            let used = 0
                            for(let i = 1;i < (pieces + 1);i++){
                                const slicing = i * 15000
                                await fetch(`${this._proto}//${this._id}/`, {method: 'POST', headers: {'X-Iden': nick, ...this._objHeader}, body: JSON.stringify({name: datas.name, piecing: 'records', pieces, piece: i, iden: useID, records: test.slice(used, slicing)})})
                                used = slicing
                            }
                        }
                    }
                    await fetch(`${this._proto}//${this._id}/`, {method: 'POST', headers: {'X-Iden': nick, ...this._objHeader}, body: JSON.stringify({session: 'stop'})})
                } else if(datas.session === 'records'){
                    if(this._debug){
                        console.log('see records', datas.records)
                    }
                    for(const useRecord of datas.records){
                        try {
                            await dataTab.add(useRecord)
                        } catch (err) {
                            if(this._debug){
                                console.error(err)
                            }
                            try {
                                const gotRecord = await dataTab.get(useRecord.iden)
                                if(gotRecord){
                                    if(gotRecord.edit < useRecord.edit){
                                        await dataTab.put(useRecord)
                                    }
                                }
                            } catch (error) {
                                if(this._debug){
                                    console.error(error)
                                }
                            }
                        }
                    }
                } else if(datas.session === 'start'){
                    if(!this._prog.has(nick)){
                        this._prog.add(nick)
                    }
                } else if(datas.session === 'stop'){
                    if(this._prog.has(nick)){
                        this._prog.delete(nick)
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
                } else if(datas.piecing === 'records'){
                    if(this._piecing.has(datas.iden)){
                        const obj = this._piecing.get(datas.iden)
                        if(!obj.arr[datas.piece - 1]){
                            obj.arr[datas.piece - 1] = datas.data
                            obj.stamp = Date.now()
                            if(obj.arr.every(Boolean)){
                                const useData = JSON.parse(obj.arr.join(''))
                                if(!useData.records.length){
                                    return
                                }
                                for(const data of useData.records){
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

    id(){
        return crypto.randomUUID()
    }

    async doIden(data){
        const arr = await (await fetch(`${this._proto}//${this._id}`, {method: 'GET', headers: {'X-Iden': 'true', 'X-Buf': 'false'}})).json()
        if(Boolean(data)){
            return arr[Math.floor(Math.random() * arr.length)]
        } else {
            return arr
        }
    }

    async doSync(idToUse, dbOrUser, recentStamp = null, count = 15){
        const dbOrUserToUse = Boolean(dbOrUser)
        const useRecords = recentStamp ? (await table.where('user').equals(this._user).sortBy('stamp').last())?.stamp : null
        if(idToUse){
            for(const table of this.db.tables){
                await fetch(`${this._proto}//${this._id}/`, {method: 'POST', headers: {'X-Iden': idToUse, ...this._objHeader}, body: JSON.stringify({name: table.name, session: 'sync', sync: dbOrUserToUse, records: useRecords || 0, count})})
            }
        } else {
            const idens = await (await fetch(`${this._proto}//${this._id}`, {method: 'GET', headers: {'X-Iden': 'true', 'X-Buf': 'false'}})).json()
            for(const iden of idens){
                for(const table of this.db.tables){
                    await fetch(`${this._proto}//${this._id}/`, {method: 'POST', headers: {'X-Iden': iden, ...this._objHeader}, body: JSON.stringify({name: table.name, session: 'sync', sync: dbOrUserToUse, records: useRecords || 0, count})})
                }
            }
        }
    }

    changeSync(tof){
        const useVar = Boolean(tof)
        if(this._sync !== useVar){
            this._sync = useVar
        }
    }

    changeTimer(sec){
        this._timer = sec || 180000
        this.turnOffInterval()
        this.turnOnInterval()
    }

    turnOnInterval(){
        if(!this._routine){
            this._routine = setInterval(() => {this.initUser().then(console.log).catch(console.error)}, this._timer)
        }
    }

    turnOffInterval(){
        if(this._routine){
            clearInterval(this._routine)
            this._routine = null
        }
    }

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
        const useData = JSON.stringify({name, user: data.user, stamp: data.stamp, iden: test, status: 'add', data})
        if(useData.length < 16000){
            await fetch(`${this._proto}//${this._id}/`, {method: 'POST', headers: this._objHeader, body: useData})
        } else {
            const pieces = Math.ceil(useData.length / 15000)
            let used = 0
            for(let i = 1;i < (pieces + 1);i++){
                const slicing = i * 15000
                await fetch(`${this._proto}//${this._id}/`, {method: 'POST', headers: this._objHeader, body: JSON.stringify({name, data: useData.slice(used, slicing), user: data.user, stamp: data.stamp, iden: test, piecing: 'add', pieces, piece: i})})
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
                await fetch(`${this._proto}//${this._id}/`, {method: 'POST', headers: this._objHeader, body: useData})
            } else {
                const pieces = Math.ceil(useData.length / 15000)
                let used = 0
                for(let i = 1;i < (pieces + 1);i++){
                    const slicing = i * 15000
                    await fetch(`${this._proto}//${this._id}/`, {method: 'POST', headers: this._objHeader, body: JSON.stringify({name, data: useData.slice(used, slicing), iden: test.iden, user: test.user, edit: data.edit, num, piecing: 'edit', pieces, piece: i})})
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
        await dataTab.delete(test.iden)
        if(test.user === this._user){
            await fetch(`${this._proto}//${this._id}/`, {method: 'POST', headers: {'X-Iden': iden, ...this._objHeader}, body: JSON.stringify({name, iden: test.iden, user: test.user, status: 'sub'})})
        }
        return test.iden
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
        await fetch(`${this._proto}//${this._id}/`, {method: 'POST', headers: {'X-Iden': iden, ...this._objHeader}, body: JSON.stringify(data)})
    }

    async getDB(blob, opts){
        return await this.db.import(blob, opts)
    }

    async postDB(opts){
        return await this.db.export(opts)
    }

    quit(){
        this.turnOffInterval()
        this.db.close()
    }
}