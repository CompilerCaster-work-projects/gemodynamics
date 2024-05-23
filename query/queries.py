import datetime
from typing import List


def pipeline(name: str, start_date: datetime, end_date: datetime, org_ids: List[int]) -> List[str]:
    pipes = {
        "sheet1": [
            {
                '$match': {
                    'timestamps.processedAt': {
                        '$lt': end_date,
                        '$gte': start_date
                    },
                    'isTest': False,
                    'organization.id':{ '$in': org_ids}
                }
            }, {
                '$project': {
                    'organization_name': '$organization.name',
                    'organization_id': '$organization.id',
                    'organization_inn': '$organization.inn',
                    'host_release_point': '$host.releasePoint.address',
                    'resolution_success': '$resolution.success',
                    'resolution_remarks': '$resolution.remarks',
                }
            }, {
                '$group': {
                    '_id': {
                        'organization_name': '$organization_name',
                        'organization_id': '$organization_id',
                        'organization_inn': '$organization_inn',
                        'host_release_point': '$host_release_point',
                    },
                    'count_medics': {
                        '$sum': 1
                    },
                    'count_success': {
                        '$sum': {"$cond": ['$resolution_success', 1, 0]}
                    },
                    'count_not_success': {
                        '$sum': {"$cond": ['$resolution_success', 0, 1]}
                    },
                    'count_med_cause': {
                        '$sum': {"$cond": [{'$and': [{'$or': [{'$in': ['pulse', '$resolution_remarks']},
                                                              {'$in': [
                                                                  'pressure', '$resolution_remarks']},
                                                              {'$in': [
                                                                  'temperature', '$resolution_remarks']},
                                                              {'$in': [
                                                                  'complaints', '$resolution_remarks']},
                                                              {'$in': [
                                                                  'complains', '$resolution_remarks']},
                                                              {'$in': [
                                                                  'skin', '$resolution_remarks']},
                                                              {'$in': [
                                                                  'injury', '$resolution_remarks']},
                                                              {'$in': [
                                                                  'alcohol', '$resolution_remarks']},
                                                              {'$in': [
                                                                  'reactions', '$resolution_remarks']},
                                                              {'$in': [
                                                                  'inebriation', '$resolution_remarks']},
                                                              {'$in': [
                                                                  'odd_behavior', '$resolution_remarks']},
                                                              ]}, {'$eq': ['$resolution_success', False]}]}, 1, 0]}
                    },
                    'count_adm_cause': {
                        '$sum': {"$cond": [{'$and': [{'$or': [{'$in': ['alco_out_of_sight', '$resolution_remarks']},
                                                              {'$in': [
                                                                  'tono_out_of_sight', '$resolution_remarks']},
                                                              {'$in': [
                                                                  'alco_rules', '$resolution_remarks']},
                                                              {'$in': [
                                                                  'tono_rules', '$resolution_remarks']},
                                                              {'$in': [
                                                                  'temperature_rules', '$resolution_remarks']},
                                                              {'$in': [
                                                                  'out_of_sight', '$resolution_remarks']},
                                                              {'$in': [
                                                                  'wrong_order', '$resolution_remarks']},
                                                              {'$in': [
                                                                  'clothes', '$resolution_remarks']},
                                                              {'$in': [
                                                                  'mask_gloves', '$resolution_remarks']},
                                                              {'$in': [
                                                                  'high_illumination', '$resolution_remarks']},
                                                              {'$in': [
                                                                  'low_illumination', '$resolution_remarks']},
                                                              {'$in': [
                                                                  'wrong_person', '$resolution_remarks']},
                                                              {'$in': [
                                                                  'strangers', '$resolution_remarks']},
                                                              {'$in': [
                                                                  'extra_stuff', '$resolution_remarks']},
                                                              {'$in': [
                                                                  'food', '$resolution_remarks']},
                                                              {'$in': [
                                                                  'bandage', '$resolution_remarks']},
                                                              {'$in': [
                                                                  'illegal_drugs_intake', '$resolution_remarks']},
                                                              ]}, {'$eq': ['$resolution_success', False]}]}, 1, 0]}
                    },
                    'count_tech_cause': {
                        '$sum': {"$cond": [{'$and': [{'$or': [{'$in': ['no_video', '$resolution_remarks']},
                                                              {'$in': [
                                                                  'outdated', '$resolution_remarks']},
                                                              {'$in': [
                                                                  'not_all_steps', '$resolution_remarks']},
                                                              {'$in': [
                                                                  'device_malfunction', '$resolution_remarks']},
                                                              {'$in': [
                                                                  'no_photo', '$resolution_remarks']},
                                                              {'$in': [
                                                                  'incorrect_video', '$resolution_remarks']},
                                                              ]}, {'$eq': ['$resolution_success', False]}]}, 1, 0]}
                    },
                    'count_cancel_cause': {
                        '$sum': {"$cond": [{'$and': [{'$in': ['cancel', '$resolution_remarks']}, {'$eq': ['$resolution_success', False]}]}, 1, 0]}
                    },
                    'count_ad': {
                        '$sum': {"$cond": [{'$and': [{'$or': [
                            {'$in': ['pressure', '$resolution_remarks']},
                        ]}, {'$eq': ['$resolution_success', False]}]}, 1, 0]}
                    },
                    'count_pulse': {
                        '$sum': {"$cond": [{'$and': [{'$or': [{'$in': ['pulse', '$resolution_remarks']}]}, {'$eq': ['$resolution_success', False]}]}, 1, 0]}
                    }

                }
            }, {
                '$project': {
                    '_id': None,
                    'organization_name': '$_id.organization_name',
                    'organization_id': '$_id.organization_id',
                    'organization_inn': '$_id.organization_inn',
                    'host_release_point': '$_id.host_release_point',
                    'count_medics': '$count_medics',
                    'count_success': '$count_success',
                    'count_not_success': '$count_not_success',
                    'count_med_cause': '$count_med_cause',
                    'count_adm_cause': '$count_adm_cause',
                    'count_tech_cause': '$count_tech_cause',
                    'count_cancel_cause': '$count_cancel_cause',
                    'count_ad': '$count_ad',
                    'count_pulse': '$count_pulse',
                }
            }],
        "sheet2": [
            {
                '$match': {
                    'timestamps.processedAt': {
                        '$lt': end_date,
                        '$gte': start_date
                    },
                    'isTest': False,
                    'type': {'$in': ['BEFORE_TRIP', 'BEFORE_SHIFT']},
                    'organization.id':{ '$in': org_ids}
                }
            }, {
                '$project': {
                    '_id': None,
                    'organization_name': '$organization.name',
                    'organization_id': '$organization.id',
                    'organization_inn': '$organization.inn',
                    'boundary_origin': '$boundaries.origin',
                    'boundary': '$boundaries.values',
                    'employee_name': '$employee.name',
                    'employee_surname': '$employee.surname',
                    'employee_patronymic': '$employee.patronymic',
                    'employee_number': '$employee.personnelNumber',
                    'employee_birthday': '$employee.dateOfBirth',
                    'resolution_remarks': '$resolution.remarks',
                    'resolution_success': '$resolution.success',
                    'dad': {'$arrayElemAt': ['$steps.result.value.pressure.diastolic', 1]},
                    'sad': {'$arrayElemAt': ['$steps.result.value.pressure.systolic', 1]},
                    'pulse': {'$arrayElemAt': ['$steps.result.value.pulse', 1]},
                }
            },
            {
                '$group': {
                    '_id': {
                        'organization_name': '$organization_name',
                        'organization_id': '$organization_id',
                        'organization_inn': '$organization_inn',
                        'boundary_origin': '$boundary_origin',
                        'boundary': '$boundary',
                        'employee_name': '$employee_name',
                        'employee_surname': '$employee_surname',
                        'employee_patronymic': '$employee_patronymic',
                        'employee_birthday': '$employee_birthday',
                        'employee_number': '$employee_number',
                    },
                    'count_all': {
                        '$sum': 1
                    },
                    'count_ad_pulse_cause': {
                        '$sum': {"$cond": [{'$and': [{'$or': [{'$in': ['pressure', '$resolution_remarks']},
                                                              {'$in': [
                                                                  'pulse', '$resolution_remarks']},
                                                              ]}, {'$eq': ['$resolution_success', False]}]}, 1, 0]}
                    },
                    'avg_sad': {
                        '$avg': '$sad'
                    },
                    'avg_dad': {
                        '$avg': '$dad'
                    },
                    'avg_pulse': {
                        '$avg': "$pulse",
                    },

                }
            }, {
                '$project': {
                    '_id': None,
                    'organization_name': '$_id.organization_name',
                    'organization_id': '$_id.organization_id',
                    'organization_inn': '$_id.organization_inn',
                    'boundary': '$_id.boundary',
                    'boundary_origin': '$_id.boundary_origin',
                    'employee_name': '$_id.employee_name',
                    'employee_surname': '$_id.employee_surname',
                    'employee_patronymic': '$_id.employee_patronymic',
                    'employee_birthday': '$_id.employee_birthday',
                    'employee_number': '$_id.employee_number',
                    'count_all': '$count_all',
                    'count_ad_pulse_cause': '$count_ad_pulse_cause',
                    'mean_sad': '$avg_sad',
                    'mean_dad': '$avg_dad',
                    'mean_pulse': '$avg_pulse',
                }}]
    }
    return pipes[name]

