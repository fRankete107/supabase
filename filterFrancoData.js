"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (g && (g = 0, op[0] && (_ = 0)), _) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
exports.__esModule = true;
exports.filterFrancoData = void 0;
var supabase_js_1 = require("@supabase/supabase-js");
require('dotenv').config();
var filterFrancoData = function () { return __awaiter(void 0, void 0, void 0, function () {
    var SUPABASE_URL, SUPABASE_KEY, supabase, data_list, error, records_viewed, same_title, same_artist, new_records, duplicate_records, _i, data_list_1, record, duplicate, last_viewed, _a, last_viewed_1, viewed, duo, timestamp1, timestamp2, dt1, dt2, delta, duo, insertError;
    return __generator(this, function (_b) {
        switch (_b.label) {
            case 0:
                SUPABASE_URL = process.env.SUPABASE_URL || 'default_value';
                SUPABASE_KEY = process.env.SUPABASE_KEY || 'default_value';
                supabase = (0, supabase_js_1.createClient)(SUPABASE_URL, SUPABASE_KEY);
                return [4 /*yield*/, supabase
                        .from("device_raw_airplay")
                        .select("*")];
            case 1:
                data_list = (_b.sent()).data;
                return [4 /*yield*/, supabase.rpc("truncate_franco_data", {})];
            case 2:
                error = (_b.sent()).error;
                if (error) {
                    console.log(error);
                    return [2 /*return*/];
                }
                if (data_list == null) {
                    return [2 /*return*/];
                }
                records_viewed = [];
                same_title = [];
                same_artist = [];
                new_records = [];
                duplicate_records = [];
                for (_i = 0, data_list_1 = data_list; _i < data_list_1.length; _i++) {
                    record = data_list_1[_i];
                    duplicate = false;
                    last_viewed = records_viewed.slice(-50);
                    for (_a = 0, last_viewed_1 = last_viewed; _a < last_viewed_1.length; _a++) {
                        viewed = last_viewed_1[_a];
                        if (viewed.market === record.market &&
                            viewed.country === record.country &&
                            viewed.station === record.station &&
                            viewed.frequency === record.frequency &&
                            viewed.title === record.title &&
                            viewed.artist === record.artist &&
                            viewed.acr_id === record.acr_id) {
                            duplicate = true;
                            duplicate_records.push(record);
                            break;
                        }
                        if (viewed.title === record.title &&
                            viewed.market === record.market &&
                            viewed.country === record.country &&
                            viewed.station === record.station &&
                            viewed.frequency === record.frequency) {
                            duo = [viewed, record];
                            same_title.push(duo);
                            timestamp1 = viewed.timestamp;
                            timestamp2 = record.timestamp;
                            if (!timestamp1) {
                                return [2 /*return*/];
                            }
                            dt1 = new Date(timestamp1);
                            dt2 = new Date(timestamp2);
                            delta = Math.abs(dt2.getTime() - dt1.getTime()) / 1000;
                            if (delta < 300) {
                                duplicate = true;
                                duplicate_records.push(record);
                                break;
                            }
                        }
                        if (viewed.artist === record.artist) {
                            duo = [viewed, record];
                            same_artist.push(duo);
                        }
                    }
                    if (!duplicate) {
                        records_viewed.push(record);
                        new_records.push(record);
                    }
                }
                return [4 /*yield*/, supabase
                        .from("franco_data")
                        .insert(new_records)];
            case 3:
                insertError = (_b.sent()).error;
                if (insertError) {
                    console.log(insertError);
                    return [2 /*return*/];
                }
                return [2 /*return*/];
        }
    });
}); };
exports.filterFrancoData = filterFrancoData;
