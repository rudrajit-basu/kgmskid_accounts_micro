'use strict'

const {Base} = require('deta');

const studentInfoDb = Base('studentInfoDB'); // access your DB
const studentMoreInfoDb = Base('studentMoreInfoDB');
const studentAccountsDb = Base('studentAccountsDB');
const studentCollectionInfoDb = Base('studentCollectionInfoDB');

const app = require('fastify')();

const Ajv = require('ajv');
const ajv = Ajv({
  // the fastify defaults (if needed)
  //removeAdditional: true,
  //useDefaults: true,
  coerceTypes: true,
  //nullable: true,
  // any other options
  // ...
});
app.setValidatorCompiler(({ schema, method, url, httpPart }) => {
  return ajv.compile(schema)
});

const uniqid = require('uniqid');

/* common functions */

const failureObj = async (sCode, err, msg) => {
	return {statusCode: sCode, error: err, message: msg};
}

const successObj = async (sCode, msg) => {
	return {statusCode: sCode, message: msg};
}

const insertManyDataToDB = async (dataArr, db) => {
	try {
		return await db.putMany(dataArr);
	} catch(err) {
		return `${err.toString()}`;
	}
}

const insertDataToDB = async (dataObj, db) => {
	try {
		return await db.put(dataObj);
	} catch(err) {
		return `${err.toString()}`;
	}
}

const fetchDataFromDb = async (query, options, db) => {
	try {
		return await db.fetch(query, options);
	} catch (err) {
		return `${err.toString()}`;
	}
}

const getTotalCountFromQuery = async (query, db) => {
	try {
		let totCount = 0;
		let runLoop = true;
		while(runLoop) {
			let {count, last} = await db.fetch(query);
			totCount += count;
			runLoop = last === undefined ? false : true;
		}
		return totCount;
	} catch (err) {
		return `${err.toString()}`;
	}
}

const updateDataToDb = async (updates, key, db) => {
	try {
		return await db.update(updates, key);
	} catch (e) {
		return `${err.toString()}`;
	}
}

const checkIfRecordPresent = async (key, db) => {
	try {
		let result = await db.get(key);
		return result != null;
	} catch (e) {
		return `${err.toString()}`;
	}
}

const getLexicoFromNum = async (numPart) => {
	const posArray = ['J', 'I' , 'H', 'G', 'F', 'E' , 'D', 'C', 'B', 'A'];
	let keyArray = [];
	let isLessThanTen = numPart < 10;
	while(numPart > 0) {
		let numPos = numPart % 10;
		keyArray.push(posArray[numPos]);
		numPart = Math.trunc(numPart / 10);
	}
	if (isLessThanTen)
		keyArray.push(posArray[0]);
	return keyArray.reverse().join('');
}

const getUniqueKeyFromDate = async (dtYear, dtMonth, dtDay) => {
	let lexiYr = await getLexicoFromNum(dtYear);
	let lexiMnt = await getLexicoFromNum(dtMonth);
	let lexiDy = await getLexicoFromNum(dtDay);
	let lexiKey = lexiYr + lexiMnt + lexiDy;
	return uniqid(lexiKey + '-');
}

const getUniqueKeyFromYear = async (dtYear) => {
	let lexiDy = await getLexicoFromNum(dtYear);
	return uniqid(lexiDy + '-');
}

const getFinalKeyForStudentAccount = async (dtYear, dtMonth, dtDay) => {
	let finalKey = await getUniqueKeyFromDate(dtYear, dtMonth, dtDay);
	while (await checkIfRecordPresent(finalKey, studentAccountsDb)) {
		finalKey = await getUniqueKeyFromDate(dtYear, dtMonth, dtDay);
	}
	return finalKey;
}

const getFinalKeyForStudentCollection = async (dtYear) => {
	let finalKey = await getUniqueKeyFromYear(dtYear);
	while (await checkIfRecordPresent(finalKey, studentCollectionInfoDb)) {
		finalKey = await getUniqueKeyFromYear(dtYear);
	}
	return finalKey;
}

const checkAndInsertStudentCollectionInfo = async (feeType, stdName, studentId, sessionYear, session, classId, className, sec) => {
	let stdCollectionInfo = await fetchDataFromDb({studentId: studentId, sessionYear: sessionYear}, {}, studentCollectionInfoDb);
	if (stdCollectionInfo.items.length === 1) {
		let {key, collectionInfo} = stdCollectionInfo.items[0];
		if (collectionInfo.hasOwnProperty(feeType)) {
			let dataArray = collectionInfo[feeType];
			let newArray = dataArray.concat(session);
			let q1 = `collectionInfo.${feeType}`;
			let updates = {};
			updates[q1] = newArray;
			await updateDataToDb(updates, key, studentCollectionInfoDb);
		} else {
			let q1 = `collectionInfo.${feeType}`;
			let updates = {};
			updates[q1] = session;
			await updateDataToDb(updates, key, studentCollectionInfoDb);
		}
	} else if (stdCollectionInfo.items.length === 0) {
		let newCollectionInfoKey = await getFinalKeyForStudentCollection(sessionYear);
		let collectionInfoData = {};
		collectionInfoData[feeType] = session;
		let newCollectionInfo = {key: newCollectionInfoKey, stdName: stdName, studentId: studentId, sessionYear: sessionYear, collectionInfo: collectionInfoData, classId: classId, className: className, sec: sec};
		await insertDataToDB(newCollectionInfo, studentCollectionInfoDb);
	}
}

const checkAndDeleteStudentCollectionInfo = async (feeType, studentId, sessionYear, session) => {
	let stdCollectionInfo = await fetchDataFromDb({studentId: studentId, sessionYear: sessionYear}, {}, studentCollectionInfoDb);
	if (stdCollectionInfo.items.length === 1) {
		let {key, collectionInfo} = stdCollectionInfo.items[0];
		if (collectionInfo.hasOwnProperty(feeType)) {
			let dataArray = collectionInfo[feeType];
			let newArray = dataArray.filter((el) => !session.includes(el));
			let q1 = `collectionInfo.${feeType}`;
			let updates = {};
			updates[q1] = newArray;
			await updateDataToDb(updates, key, studentCollectionInfoDb);
		}
	}
}

/* common functions end */

/* insert student */

const postInsertStudentOption = {
	schema: {
		body: {
			type: 'object',
			required: ['studentInfo', 'studentMoreInfo', 'studentAdmissionFeeInfo', 'studentTuitionFeeInfo'],
			properties: {
				studentInfo: {
					type: 'object',
					required: ['stdLoginId', 'name', 'className', 'classId', 'section', 'rollNo', 'doa', 'password', 'isSync', 'isActive'],
					properties: {
						stdLoginId: {type: 'string', minLength: 7},
						name: {type: 'string'},
						className: {type: 'string'},
						classId: {type: 'string'},
						section: {type: 'string'},
						rollNo: {type: 'number'},
						doa: {type: 'string'},
						password: {type: 'string', minLength: 5},
						isSync: {type: 'boolean'},
						isActive: {type: 'boolean'},
					}
				},
				studentMoreInfo: {
					type: 'object',
					required: ['medium', 'secondLang', 'dob', 'fatherName', 'motherName', 'lgName', 'contact1', 'address1', 'isSync', 'isActive'],
					properties: {
						medium: {type: 'string'},
						secondLang: {type: 'string'},
						dob: {type: 'string'},
						fatherName: {type: 'string'},
						motherName: {type: 'string'},
						lgName: {type: 'string'},
						contact1: {type: 'string'},
						contact2: {type: 'string'},
						emailId: {type: 'string'},
						address1: {type: 'string'},
						address2: {type: 'string'},
						isSync: {type: 'boolean'},
						isActive: {type: 'boolean'},
					}
				},
				studentAdmissionFeeInfo: {
					type: 'object',
					required: ['feeType', 'amount', 'isInstallment', 'dtDay', 'dtMonth', 'dtYear', 'classId', 'className', 'sec', 'installmentId', 'isSync', 'isActive', 'session', 'sessionYear'],
					properties: {
						feeType: {type: 'string', pattern: 'admissionFee'},
						amount: {type: 'number'},
						isInstallment: {type: 'boolean'},
						dtDay: {type: 'number'},
						dtMonth: {type: 'number'},
						dtYear: {type: 'number'},
						classId: {type: 'string'},
						className: {type: 'string'},
						sec: {type: 'string'},
						installmentId: {type: 'string'},
						isSync: {type: 'boolean'},
						isActive: {type: 'boolean'},
						session: {type: 'array', items: {type: "integer"}, minItems: 1},
						sessionYear: {type: 'number'},
					}
				},
				studentTuitionFeeInfo: {
					type: 'object',
					required: ['feeType', 'amount', 'isInstallment', 'dtDay', 'dtMonth', 'dtYear', 'classId', 'className', 'sec', 'installmentId', 'isSync', 'isActive', 'session', 'sessionYear'],
					properties: {
						feeType: {type: 'string', pattern: 'tuitionFee'},
						amount: {type: 'number'},
						isInstallment: {type: 'boolean'},
						dtDay: {type: 'number'},
						dtMonth: {type: 'number'},
						dtYear: {type: 'number'},
						classId: {type: 'string'},
						className: {type: 'string'},
						sec: {type: 'string'},
						installmentId: {type: 'string'},
						isSync: {type: 'boolean'},
						isActive: {type: 'boolean'},
						session: {type: 'array', items: {type: "integer"}, minItems: 1},
						sessionYear: {type: 'number'},
					}
				}
			}
		}
	}
}

const getNumPartFromLoginId = async (id) => {
	const idLen = id.length;
	const numPartLen = idLen - 6;
	return {part1: id.substring(0, numPartLen), part2: id.substring(numPartLen)};
}

const checkAndManageStdLoginId = async (stdLoginId) => {
	const {part1, part2} = await getNumPartFromLoginId(stdLoginId);
	let numPart2 = parseInt(part2);
	let counter = -1;
	while(counter !== 0) {
		let {count} = await studentInfoDb.fetch({"stdLoginId": part1+numPart2}, {});
		if (count !== 0) {
			numPart2 += 3*count;
		} else {
			counter = count;
		}
	}
	return part1+numPart2;
}

const insertStudentToDb = async (reqJsonObj) => {
	const stdCheckedLoginId = await checkAndManageStdLoginId(reqJsonObj.studentInfo.stdLoginId);
	reqJsonObj.studentInfo.stdLoginId = stdCheckedLoginId;
	const stdDbResp = await insertDataToDB(reqJsonObj.studentInfo, studentInfoDb);
	const {key, name} = stdDbResp;
	if (key !== null && key !== undefined && typeof key === 'string') {
		reqJsonObj.studentMoreInfo.studentId = key;
		const stdMoreInfoDbResp = await insertDataToDB(reqJsonObj.studentMoreInfo, studentMoreInfoDb);
		if (stdMoreInfoDbResp.hasOwnProperty('key')) {
			let admissionFeeKey = await getFinalKeyForStudentAccount(reqJsonObj.studentAdmissionFeeInfo.dtYear, reqJsonObj.studentAdmissionFeeInfo.dtMonth, reqJsonObj.studentAdmissionFeeInfo.dtDay);
			let tuitionFeeKey = await getFinalKeyForStudentAccount(reqJsonObj.studentTuitionFeeInfo.dtYear, reqJsonObj.studentTuitionFeeInfo.dtMonth, reqJsonObj.studentTuitionFeeInfo.dtDay);
			const accountsArray = [];
			reqJsonObj.studentAdmissionFeeInfo.key = admissionFeeKey;
			reqJsonObj.studentTuitionFeeInfo.key = tuitionFeeKey;
			reqJsonObj.studentAdmissionFeeInfo.studentId = key;
			reqJsonObj.studentTuitionFeeInfo.studentId = key;
			reqJsonObj.studentAdmissionFeeInfo.stdName = name;
			reqJsonObj.studentTuitionFeeInfo.stdName = name;
			accountsArray.push(reqJsonObj.studentAdmissionFeeInfo);
			accountsArray.push(reqJsonObj.studentTuitionFeeInfo);
			const stdAccDbResp = await insertManyDataToDB(accountsArray, studentAccountsDb);
			if (stdAccDbResp.processed !== undefined && stdAccDbResp.processed.items.length === 2) {
				await checkAndInsertStudentCollectionInfo(reqJsonObj.studentAdmissionFeeInfo.feeType, name, key, reqJsonObj.studentAdmissionFeeInfo.sessionYear, reqJsonObj.studentAdmissionFeeInfo.session, reqJsonObj.studentAdmissionFeeInfo.classId, reqJsonObj.studentAdmissionFeeInfo.className, reqJsonObj.studentAdmissionFeeInfo.sec);
				await checkAndInsertStudentCollectionInfo(reqJsonObj.studentTuitionFeeInfo.feeType, name, key, reqJsonObj.studentTuitionFeeInfo.sessionYear, reqJsonObj.studentTuitionFeeInfo.session, reqJsonObj.studentTuitionFeeInfo.classId, reqJsonObj.studentTuitionFeeInfo.className, reqJsonObj.studentTuitionFeeInfo.sec);
				return await successObj(201, 'success');
			} else {
				return await failureObj(502, stdAccDbResp, 'found at studentAccountsDb');
			}
		} else {
			return await failureObj(502, stdMoreInfoDbResp, 'found at studentMoreInfoDb');
		}
	} else {
		return await failureObj(502, stdDbResp, 'found at studentInfoDb');
	}
}

async function handleInsertStudent (request, reply) {
	let resp = await insertStudentToDb(request.body);
	reply.code(resp.statusCode).send(resp);
}

app.post('/insertStudent', postInsertStudentOption, handleInsertStudent);

/* insert student end */

/* api test call */

app.get('/test', async (request, reply) => {
	reply.code(200).send({message: 'success'});
});

/* api test call end */

/* retrieve students basic info */

const postGetStudentsOptions = {
	schema: {
		body: {
			type: 'object',
			required: ['classId', 'section', 'limit' , 'last', 'name', 'isActive'],
			properties: {
				classId: {type: 'string'},
				section: {type: 'string'},
				limit: {type: 'number', minimum: 0, maximum: 30},
				last: {type: 'string'},
				name: {type: 'string'},
				isActive: {type: 'boolean'}
			}
		}
	}
}

const getQueryToFetchStudents = async (reqJsonObj) => {
	const {classId, section, limit, last, name, isActive}  = reqJsonObj;
	const query = {};
	const options = {};
	let isTotalCountRequired = false;
	query.isActive = isActive;
	if (classId !== '') {
		query.classId = classId;
	}
	if (section !== '') {
		query.section = section;
	}
	if (name !== '') {
		query["name?contains"] = name;
	}
	if (limit !== 0) {
		options.limit = limit;
	}
	if (last !== '') {
		options.last = last;
	}
	if (options.limit && !options.last && !query["name?contains"]) {
		isTotalCountRequired = true;
	}
	return {query: query, options: options, isTotalCount: isTotalCountRequired}; 
}

async function handleGetStudents (request, reply) {
	let {query, options, isTotalCount} = await getQueryToFetchStudents(request.body);
	const res = await fetchDataFromDb(query, options, studentInfoDb);
	if (res.hasOwnProperty('items')) {
		if (isTotalCount) {
			let totalCount = await getTotalCountFromQuery(query, studentInfoDb);
			res.totalCount = totalCount;
		}
		reply.code(200).send(res);
	} else {
		reply.code(502).send(await failureObj(502, res, 'found at studentInfoDb'));
	}
}

app.post('/getStudents', postGetStudentsOptions, handleGetStudents);

/* retrieve students basic info end */

/* retrieve students more info */

const getStudentMoreInfoOptions = {
	schema: {
		body: {
			type: 'object',
			required: ['stdId'],
			properties: {
				stdId: {type: 'string', pattern: '^[A-Za-z0-9]+$'}
			}
		}
	}	
}

async function handleGetStudentMoreInfo (request, reply) {
	let {stdId}  = request.body;
	const result = await fetchDataFromDb({studentId: stdId}, {}, studentMoreInfoDb);
	if (result.hasOwnProperty('items')) {
		reply.code(200).send(result);
	} else {
		reply.code(502).send(await failureObj(502, result, 'found at studentInfoDb'));
	}
}

app.post('/getStudentMoreInfo', getStudentMoreInfoOptions, handleGetStudentMoreInfo);

/* retrieve students more info end */

/* update student basic details */

const updateStudentBasicInfoOptions = {
	schema: {
		body: {
			type: 'object',
			required: ['studentInfoKey', 'studentInfo', 'studentMoreInfoKey', 'studentMoreInfo'],
			properties: {
				studentInfoKey: {type: 'string'},
				studentInfo: {
					type: 'object',
					properties: {
						stdLoginId: {type: 'string', minLength: 7},
						password: {type: 'string', minLength: 5},
					}
				},
				studentMoreInfoKey: {type: 'string'},
				studentMoreInfo: {type: 'object'}
			}
		}
	}
}

const updateStudentBasicInfo = async (reqObj) => {
	let errorMsg = '';
	let isInputValid = false;
	let {studentInfoKey, studentInfo, studentMoreInfoKey, studentMoreInfo} = reqObj;
	studentInfoKey = studentInfoKey.trim();
	studentMoreInfoKey = studentMoreInfoKey.trim();
	if (studentInfoKey !== '' && Object.keys(studentInfo).length !== 0 && studentInfo.constructor === Object) {
		if (studentInfo.hasOwnProperty('stdLoginId')) {
			let stdId = await checkAndManageStdLoginId(studentInfo.stdLoginId);
			studentInfo.stdLoginId = stdId;
		}
		let result = await updateDataToDb(studentInfo, studentInfoKey, studentInfoDb);
		if (result !== null) {
			errorMsg = errorMsg + ' studentInfoDb: ' + result;
		}
		isInputValid = true;
	}
	
	if (studentMoreInfoKey !== '' && Object.keys(studentMoreInfo).length !== 0 && studentMoreInfo.constructor === Object) {
		let result = await updateDataToDb(studentMoreInfo, studentMoreInfoKey, studentMoreInfoDb);
		if (result !== null) {
			errorMsg = errorMsg + ' studentMoreInfoDb: ' + result;
		}
		isInputValid = true;
	}
	
	if (errorMsg === '') {
		if (isInputValid) {
			if (studentInfo.hasOwnProperty('className') || studentInfo.hasOwnProperty('classId') || studentInfo.hasOwnProperty('section')) {
				let updates = {};
				if (studentInfo.hasOwnProperty('className')) updates['className'] = studentInfo.className;
				if (studentInfo.hasOwnProperty('classId')) updates['classId'] = studentInfo.classId;
				if (studentInfo.hasOwnProperty('section')) updates['sec'] = studentInfo.section;
				let collectionInfoResult = await fetchDataFromDb({studentId: studentInfoKey}, {limit: 1}, studentCollectionInfoDb);
				if (collectionInfoResult.hasOwnProperty('items') && collectionInfoResult.items.length > 0) {
					let {key} = collectionInfoResult.items[0];
					await updateDataToDb(updates, key, studentCollectionInfoDb);	
				}
			}
			return await successObj(200, 'success');	
		} else {
			return await failureObj(400, 'invalid input', 'no data to update');	
		}
	} else {
		return await failureObj(502, errorMsg, 'error while student basic update');
	}
}

async function handleUpdateStudentBasicInfo (request, reply) {
	let resp = await updateStudentBasicInfo(request.body);
	reply.code(resp.statusCode).send(resp);
}

app.post('/updateStudentBasicInfo', updateStudentBasicInfoOptions, handleUpdateStudentBasicInfo);

/* update student basic details end */

/* insert students account details */

const insertStudentAccountInfoOptions = {
	schema: {
		body: {
			type: 'object',
			required: ['feeType', 'amount', 'isInstallment', 'dtDay', 'dtMonth', 'dtYear', 'classId', 'className', 'sec', 'installmentId', 'isSync', 'isActive', 'studentId', 'stdName', 'session', 'sessionYear'],
			properties: {
				feeType: {type: 'string'},
				amount: {type: 'number'},
				isInstallment: {type: 'boolean'},
				dtDay: {type: 'number'},
				dtMonth: {type: 'number'},
				dtYear: {type: 'number'},
				classId: {type: 'string'},
				className: {type: 'string'},
				sec: {type: 'string'},
				installmentId: {type: 'string'},
				isSync: {type: 'boolean'},
				isActive: {type: 'boolean'},
				studentId: {type: 'string', pattern: '^[A-Za-z0-9]+$'},
				stdName: {type: 'string'},
				session: {type: 'array', items: {type: "integer"}, minItems: 1},
				sessionYear: {type: 'number'},
			}
		}
	}
}

const insertStudentAccountInfoToDb = async (reqJsonObj) => {
	let finalKey = await getFinalKeyForStudentAccount(reqJsonObj.dtYear, reqJsonObj.dtMonth, reqJsonObj.dtDay);
	reqJsonObj.key = finalKey;
	const stdAccDbResp = await insertDataToDB(reqJsonObj, studentAccountsDb);
	if(stdAccDbResp.hasOwnProperty('key')) {
		if (reqJsonObj.installmentId === '')
			await checkAndInsertStudentCollectionInfo(reqJsonObj.feeType, reqJsonObj.stdName, reqJsonObj.studentId, reqJsonObj.sessionYear, reqJsonObj.session, reqJsonObj.classId, reqJsonObj.className, reqJsonObj.sec);
		return await successObj(201, 'success');
	}	
	else {
		return await failureObj(502, stdAccDbResp, 'found at studentAccountsDb');
	}	
}

async function handleInsertStudentAccountInfo (request, reply) {
	let resp = await insertStudentAccountInfoToDb(request.body);
	reply.code(resp.statusCode).send(resp);
}

app.post('/insertStudentAccountInfo', insertStudentAccountInfoOptions, handleInsertStudentAccountInfo);

/* insert students account details end */

/* retrieve students account details */

const getStudentAccountInfoOptions = {
	schema: {
		body: {
			type: 'object',
			required: ['studentId', 'installmentId', 'isActive', 'limit', 'last', 'dtDay', 'dtMonth', 'dtYear'],
			properties: {
				studentId: {type: 'string', pattern: '^[A-Za-z0-9]+$'},
				installmentId: {type: 'string'},
				isActive: {type: 'boolean'},
				limit: {type: 'number', minimum: 0, maximum: 30},
				last: {type: 'string'},
				dtDay: {type: 'number', minimum: 0},
				dtMonth: {type: 'number', minimum: 0},
				dtYear: {type: 'number', minimum: 0}
			}
		}
	}
}

const getQueryToFetchStudentAccounts = async (reqJsonObj) => {
	let {studentId, installmentId, isActive, limit, last, dtDay, dtMonth, dtYear} = reqJsonObj;
	const query = {};
	const options = {};
	let isTotalCountRequired = false;
	query.studentId = studentId;
	query.isActive = isActive;
	if (installmentId !== '')
		query.installmentId = installmentId;
	if (dtDay !== 0)
		query.dtDay = dtDay;
	if (dtMonth !== 0)
		query.dtMonth = dtMonth;
	if (dtYear !== 0)
		query.dtYear = dtYear;
	if (limit !== 0)
		options.limit = limit;
	if(last !== '')
		options.last = last;
	if (options.limit && !options.last)
		isTotalCountRequired = true;
	return {query: query, options: options, isTotalCount: isTotalCountRequired};
}

async function handleGetStudentAccountInfo (request, reply) {
	let {query, options, isTotalCount} = await getQueryToFetchStudentAccounts(request.body);
	const res = await fetchDataFromDb(query, options, studentAccountsDb);
	if (res.hasOwnProperty('items')) {
		if (isTotalCount) {
			let totalCount = await getTotalCountFromQuery(query, studentAccountsDb);
			res.totalCount = totalCount;
		}
		reply.code(200).send(res);
	} else {
		reply.code(502).send(await failureObj(502, res, 'found at studentAccountsDb'));
	}
}

app.post('/getStudentAccountInfo', getStudentAccountInfoOptions, handleGetStudentAccountInfo);

/* retrieve students account details end */

/* retrieve students account installment details */

const getStudentAccountInstallmentInfoOptions = {
	schema: {
		body: {
			type: 'object',
			required: ['installmentId', 'isActive'],
			properties: {
				installmentId: {type: 'string', minLength: 12},
				isActive: {type: 'boolean'}
			}
		}
	}
}

async function handleGetStudentAccountInstallmentInfo (request, reply) {
	let {installmentId, isActive} = request.body;
	let query = [{installmentId: installmentId, isActive: isActive}];
	const res = await fetchDataFromDb(query, {}, studentAccountsDb);
	if (res.hasOwnProperty('items')) {
		const keyRes = await studentAccountsDb.get(installmentId);
		if (keyRes != null) {
			res.items.push(keyRes);
			res.count = res.count + 1;
		}
		reply.code(200).send(res);
	} else {
		reply.code(502).send(await failureObj(502, res, 'found at studentAccountsDb'));
	}
}

app.post('/getStudentAccountInstallmentInfo', getStudentAccountInstallmentInfoOptions, handleGetStudentAccountInstallmentInfo);

/* retrieve students account installment details end */

/* update status students account details */

const updateStudentAccountStatusInfoOptions = {
	schema: {
		body: {
			type: 'object',
			required: ['keyId', 'isActive', 'isMainInstallment', 'hasInstallmentId'],
			properties: {
				keyId: {type: 'string', minLength: 12},
				isActive: {type: 'boolean'},
				isMainInstallment: {type: 'boolean'},
				hasInstallmentId: {type: 'boolean'},
			}
		}
	}
}

const updateMultipleDataToDb = async (updates, keyArr, db) => {
	let errLog = [];
	for (let i = 0; i < keyArr.length; i++) {
		let res = await updateDataToDb(updates, keyArr[i], db);
		if (res !== null) {
			errLog.push(res);
		}
	}
	return errLog.length > 0 ? errLog.join(" & ") : null;
}

const updateStudentAccountStatus = async (keyId, isActive, isMainInstallment) => {
	if (isMainInstallment) {
		let query = [{installmentId: keyId, isActive: !isActive}];
		let queryResult = await fetchDataFromDb(query, {}, studentAccountsDb);
		if (queryResult.hasOwnProperty('items')) {
			let keyArr = [];
			keyArr.push(keyId);
			for (let i = 0; i < queryResult.items.length; i++) {
				keyArr.push(queryResult.items[i].key);
			}
			return await updateMultipleDataToDb({isActive: isActive}, keyArr, studentAccountsDb);
		} else {
			return queryResult;
		}
	} else {
		let res = await updateDataToDb({isActive: isActive}, keyId, studentAccountsDb);
		return res;
	}
}

const updateStudentCollectionInfo = async (keyId, isActive) => {
	let accRes = await studentAccountsDb.get(keyId);
	if (accRes !== null && accRes.hasOwnProperty('key')) {
		let {feeType, stdName, studentId, sessionYear, session, classId, className, sec} = accRes;
		if (isActive) {
			// insert
			await checkAndInsertStudentCollectionInfo(feeType, stdName, studentId, sessionYear, session, classId, className, sec);
		} else {
			// delete
			await checkAndDeleteStudentCollectionInfo(feeType, studentId, sessionYear, session);
		}
	}
}

async function handleUpdateStudentAccountStatusInfo (request, reply) {
	let {keyId, isActive, isMainInstallment, hasInstallmentId} = request.body;
	const res = await updateStudentAccountStatus(keyId, isActive, isMainInstallment);
	if (res === null) {
		if (!hasInstallmentId)
			await updateStudentCollectionInfo(keyId, isActive);
		reply.code(200).send(await successObj(200, 'success'));
	} else {
		reply.code(502).send(await failureObj(502, res, 'found at studentAccountsDb'));
	}
}

app.post('/updateStudentAccountStatusInfo', updateStudentAccountStatusInfoOptions, handleUpdateStudentAccountStatusInfo);

/* update status students account details end */

/* update status students basic info */

const updateStudentBasicStatusInfoOptions = {
	schema: {
		body: {
			type: 'object',
			required: ['keyId', 'isActive'],
			properties: {
				keyId: {type: 'string', minLength: 12},
				isActive: {type: 'boolean'}
			}
		}
	}
}

async function handleUpdateStudentBasicStatusInfo (request, reply) {
	let {keyId, isActive} = request.body;
	const res = await updateDataToDb({isActive: isActive}, keyId, studentInfoDb);
	if (res === null) {
		reply.code(200).send(await successObj(200, 'success'));
	} else {
		reply.code(502).send(await failureObj(502, res, 'found at studentInfoDb'));
	}
}

app.post('/updateStudentBasicStatusInfo', updateStudentBasicStatusInfoOptions, handleUpdateStudentBasicStatusInfo);

/* update status students basic info end */

/* delete students account details */

const deleteStudentAccountInfoOptions = {
	schema: {
		body: {
			type: 'object',
			required: ['keyId', 'isMainInstallment'],
			properties: {
				keyId: {type: 'string', minLength: 12},
				isMainInstallment: {type: 'boolean'},
			}
		}
	}
}

const deleteInActiveAccountInfo = async (keyId, isMainInstallment) => {
	if (isMainInstallment) {
		let query = [{installmentId: keyId, isActive: false}];
		let queryResult = await fetchDataFromDb(query, {}, studentAccountsDb);
		if (queryResult.hasOwnProperty('items')) {
			let keyArr = [];
			for (let i = 0; i < queryResult.items.length; i++) {
				keyArr.push(queryResult.items[i].key);
			}
			keyArr.push(keyId);
			for (let j = 0; j < keyArr.length; j++) {
				await studentAccountsDb.delete(keyArr[j]);
			}
			return null;
		} else {
			return 'queryResult returned null';
		}
	} else {
		return await studentAccountsDb.delete(keyId);
	}
}

async function handleDeleteStudentAccountInfo (request, reply) {
	let {keyId, isMainInstallment} = request.body;
	let res = await deleteInActiveAccountInfo(keyId, isMainInstallment);
	if (res === null) {
		reply.code(200).send(await successObj(200, 'success'));
	} else {
		reply.code(502).send(await failureObj(502, res, 'found at studentInfoDb'));
	}
}

app.post('/deleteStudentAccountInfo', deleteStudentAccountInfoOptions, handleDeleteStudentAccountInfo);

/* delete students account details end */

/* get student collection report */

const getStudentCollectionReportOptions = {
	schema: {
		body: {
			type: 'object',
			required: ['feeType', 'sessionYear', 'session'],
			properties: {
				feeType: {type: 'string'},
				sessionYear: {type: 'number'},
				session: {type: 'array', items: {type: "integer"}, minItems: 1},				
			}
		}
	}
}

const checkFeeTypeAndMatchSession = async (feeType, session, collectionObj) => {
	let result = false;
	if (collectionObj.hasOwnProperty('collectionInfo') && collectionObj.collectionInfo.hasOwnProperty(feeType)) {
		let collectionFeeTypeArr = collectionObj.collectionInfo[feeType];
		let containedArr = session.filter((el) => collectionFeeTypeArr.includes(el));
		containedArr.sort((a, b) => {return a- b});
		session.sort((a, b) => {return a- b});
		if (JSON.stringify(session) === JSON.stringify(containedArr))
			result = true;
		else
			result = false;
	}
	return result;
}

const getCollectionCountClasswise = async (feeType, sessionYear, session) => {
	let query = {sessionYear: sessionYear};
	let result = {};
	let paidList = {};
	let unPaidList = {};
	let runLoop = true;
	while(runLoop) {
		let {items, last} = await studentCollectionInfoDb.fetch(query);
		for (let i = 0; i < items.length; i++) {
			if (await checkFeeTypeAndMatchSession(feeType, session, items[i])) {
				if (paidList[items[i]['className']])
					paidList[items[i]['className']] = paidList[items[i]['className']] + 1;
				else
					paidList[items[i]['className']] = 1;	
			} else {
				let unpaidClassName = items[i]['className'] + '-unpaid';
				if (unPaidList[unpaidClassName])
					unPaidList[unpaidClassName] = unPaidList[unpaidClassName] + 1;
				else
					unPaidList[unpaidClassName] = 1;
			}
		}
		runLoop = last === undefined ? false : true;
		if (!runLoop) break;
	}
	result['paid'] = paidList;
	result['unPaid'] = unPaidList;
	return result;
}

async function handleGetStudentCollectionReport (request, reply) {
	let {feeType, sessionYear, session} = request.body;
	let result = await getCollectionCountClasswise(feeType, sessionYear, session);
	reply.code(200).send(result);
}

app.post('/getStudentCollectionReport', getStudentCollectionReportOptions, handleGetStudentCollectionReport);

/* get student collection report end */

/* retrieve student collection info */

const getStudentCollectionInfoOptions = {
	schema: {
		body: {
			type: 'object',
			required: ['feeType', 'sessionYear', 'session', 'className', 'isPaid', 'limit', 'last'],
			properties: {
				feeType: {type: 'string'},
				sessionYear: {type: 'number'},
				session: {type: 'array', items: {type: "integer"}, minItems: 1},
				className: {type: 'string'},
				isPaid: {type: 'boolean'},
				limit: {type: 'number', minimum: 1, maximum: 30},
				last: {type: 'string'}
			}
		}
	}
}

const getStudentWiseCollectionInfo = async (reqJsonObj) => {
	let {feeType, sessionYear, session, className, isPaid, limit, last} = reqJsonObj;
	let query = {sessionYear: sessionYear, className: className};
	let options = {};
	if(last !== '')
		options.last = last;
	
	let finalItems = [];
	let finalLast = undefined;
	let finalResult = {};
	let runLoop = true;
	while(runLoop) {
		let {items, last} = await studentCollectionInfoDb.fetch(query, options);
		if (isPaid) {
			for (let i = 0; i < items.length; i++) {
				if (finalItems.length < limit) {
					let res = await checkFeeTypeAndMatchSession(feeType, session, items[i]);
					if (res)
						finalItems.push(items[i]);
				} else {
					runLoop = false;
					break;
				}
			}
		} else {
			for (let i = 0; i < items.length; i++) {
				if (finalItems.length < limit) {
					let res = await checkFeeTypeAndMatchSession(feeType, session, items[i]);
					if (!res)
						finalItems.push(items[i]);
				} else {
					runLoop = false;
					break;
				} 
			}
		}
		if (last !== undefined) {
			options.last = last;
		} else {
			runLoop = false;
		}
		if (!runLoop || finalItems.length === limit) break;
	}
	
	finalResult.items = finalItems;
	if (finalItems.length > 0)
	finalResult.last = finalItems[finalItems.length - 1].key;
	
	return finalResult;
}

async function handleGetStudentCollectionInfo (request, reply) {
	let res = await getStudentWiseCollectionInfo(request.body);
	reply.code(200).send(res);
}

app.post('/getStudentCollectionInfo', getStudentCollectionInfoOptions, handleGetStudentCollectionInfo);

/* retrieve student collection info end */

/* retrieve account transaction details */

const getAccountTransactionDetailsOptions = {
	schema: {
		body: {
			type: 'object',
			required: ['feeType', 'dtDay', 'dtMonth', 'dtYear', 'classId', 'className', 'sec', 'stdName', 'session', 'sessionYear', 'limit', 'last'],
			properties: {
				feeType: {type: 'string'},
				dtDay: {type: 'number'},
				dtMonth: {type: 'number'},
				dtYear: {type: 'number'},
				classId: {type: 'string'},
				className: {type: 'string'},
				sec: {type: 'string'},
				stdName: {type: 'string'},
				session: {type: 'array', items: {type: "integer"}, minItems: 0},
				sessionYear: {type: 'number'},
				limit: {type: 'number', minimum: 1, maximum: 30},
				last: {type: 'string'}
			}
		}
	}
}

const getQueryToFetchAccountTransaction = async (reqJsonObj) => {
	let {feeType, dtDay, dtMonth, dtYear, classId, className, sec, stdName, session, sessionYear, limit, last} = reqJsonObj;
	const query = {};
	const options = {};
	let isTotalCountRequired = false;
	query.isActive = true;
	if (feeType !== '')
		query.feeType = feeType;
	if (dtDay !== 0)
		query.dtDay = dtDay;
	if (dtMonth !== 0)
		query.dtMonth = dtMonth;
	if (dtYear !== 0)
		query.dtYear = dtYear;
	if (classId !== '')
		query.classId = classId;
	if (className !== '')
		query.className = className;
	if (sec !== '')
		query.sec = sec;
	if (stdName !== '')
		query["stdName?contains"] = stdName;
	if (session.length > 0)
		query.session = session;
	if (sessionYear > 0)
		query.sessionYear = sessionYear;
	if (limit !== 0)
		options.limit = limit;
	if(last !== '')
		options.last = last;
	if (options.limit && !options.last && !query["stdName?contains"] && !query.session)
		isTotalCountRequired = true;
	return {query: query, options: options, isTotalCount: isTotalCountRequired};
}

const checkAndMatchSessionForAccounts = async (session, accountObj) => {
	let result = false;
	if (accountObj.hasOwnProperty('session')) {
		let accountSessionArr = accountObj.session;
		let containedArr = session.filter((el) => accountSessionArr.includes(el));
		containedArr.sort((a, b) => {return a- b});
		session.sort((a, b) => {return a- b});
		if (JSON.stringify(session) === JSON.stringify(containedArr))
			result = true;
		else
			result = false;
	}
	return result;
}

async function handleGetAccountTransactionDetails (request, reply) {
	let reqData = await getQueryToFetchAccountTransaction(request.body);
	if (reqData.query.session) {
		let reqSession = reqData.query.session;
		delete reqData.query.session;
		let query = reqData.query;
		let options = reqData.options;
		let runLoop = true;
		let finalItems = [];
		let finalResult = {};
		while (runLoop) {
			let {items, last} = await studentAccountsDb.fetch(query, options);
			for (let i = 0; i < items.length; i++) {
				if (finalItems.length < options.limit) {
					if (await checkAndMatchSessionForAccounts(reqSession, items[i]))
						finalItems.push(items[i]);	
				} else {
					runLoop = false;
					break;
				}
			}
			if (last !== undefined) {
				options.last = last;
			} else {
				runLoop = false;
			}
			if (!runLoop || finalItems.length === options.limit) break;
		}
		finalResult.items = finalItems;
		if (finalItems.length > 0)
			finalResult.last = finalItems[finalItems.length - 1].key;
		reply.code(200).send(finalResult);
	} else {
		let query = reqData.query;
		let options = reqData.options;
		let isTotalCount = reqData.isTotalCount;
		const res = await fetchDataFromDb(query, options, studentAccountsDb);
		if (res.hasOwnProperty('items')) {
			if (isTotalCount) {
				let totalCount = await getTotalCountFromQuery(query, studentAccountsDb);
				res.totalCount = totalCount;
			}
			reply.code(200).send(res);
		} else {
			reply.code(502).send(await failureObj(502, res, 'found at studentAccountsDb'));
		}
	}
}

app.post('/getAccountTransactionDetails', getAccountTransactionDetailsOptions, handleGetAccountTransactionDetails);
/* retrieve account transaction details end */

// export 'app'
module.exports = app;