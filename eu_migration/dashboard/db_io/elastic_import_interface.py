from elasticsearch import helpers

from helper.logger import Logger


def del_index(es_obj, index_name, del_existing_index):
    # delete a specified index from the database
    # allocate memory for the response
    res = {}

    if del_existing_index:
        response_confirm = input("Confirm delete the existing \'%s\' index? [y]/n: " % index_name)
        if response_confirm == 'y':
            # delete the specified index if it exists
            if es_obj.indices.exists(index_name):
                Logger.log.info('Deleting %s index...' % index_name)
                res = es_obj.indices.delete(index=index_name, ignore=[400, 404])
                Logger.log.info('%s' % res)

    return res


def force_del_index(es_obj, index_name):
    # delete a specified index from the database
    # allocate memory for the response
    res = {}

    # flag to force the deletion of the index without user input
    if force_del_index:
        if es_obj.indices.exists(index_name):
            Logger.log.info('Deleting %s index...' % index_name)
            res = es_obj.indices.delete(index=index_name, ignore=[400, 404])
            Logger.log.info('%s' % res)

        return res

    return res


def del_index_no_prompt(es_obj, index_name):
    # delete a specified index from the database
    # allocate memory for the response
    res = {}

    # delete the specified index if it exists
    if es_obj.indices.exists(index_name):
        Logger.log.info('Deleting %s index...' % index_name)
        res = es_obj.indices.delete(index=index_name, ignore=[400, 404])
        Logger.log.info('%s' % res)

    return res


def update_docs_in_index(es, update_index_name, docs_list, update_field_name, update_value):
    # inputs:
    #     es - elasticsearch database connection
    #     update_index_name - name of index in which documents will be updated
    #     docs_list - list of documents to be updated (list of dictionaries)
    #     update_field_name - field to update or add to documents in the index
    #     update_value - value to assign for the field to the document

    results = []
    n_docs = len(docs_list)

    ctr = 0
    for doc in docs_list:
        # ctr += 1
        # if ctr % 10 == 0:
        #     Logger.log.info("Percent completion: %f" % (100.0 * ctr / n_docs))

        if update_value == None:
            update_value = ''

        if type(update_value) == float:
            update_body = """
                                {
                                    "doc": {
                                        "%s": %f
                                    }
                                }
                            """ % (update_field_name, update_value)
        elif type(update_value) == int:
            update_body = """
                                {
                                    "doc": {
                                        "%s": %d
                                    }
                                }
                          """ % (update_field_name, update_value)
        else:
            update_body = """
                        {
                            "doc": {
                                "%s": "%s"
                            }
                        }
                    """ % (update_field_name, update_value)

        results += es.update(index=update_index_name, id=doc["_id"], body=update_body)

    return results


def insert_geo_dict_doc(geo_obj_dicts, es_obj, index_name, n_docs_bulk, retain_lat_lon=False):
    # perform bulk inserts for any index that contains a single 'geopoint' type as an identifier
    # the input data should be matched with an index mapping that defines a key called 'position' as type geopoint
    # the input data variable is 'geo_obj_dicts' this should contain keys that exactly match 'lat' and 'lon' which
    #   are translated to a 'geopoint' data type, other data types are define in the mapping function

    # pp = pprint.PrettyPrinter(indent=4)

    # setup the index for bulk imports
    settings_temp = {
        "settings": {
            "index": {
                "refresh_interval": -1,
                "number_of_replicas": 0}
        }}
    res = es_obj.indices.put_settings(body=settings_temp, index=index_name)

    # calculate the number of documents
    n_docs = len(geo_obj_dicts)
    # compute data attributes for the file
    actions_list = []
    n_success = 0
    for geo_obj_dict in geo_obj_dicts:
        # define the database action
        action_str = {"_index": index_name,
                      "_source": {"position": {}}
                      }

        # insert data into the action
        for key in geo_obj_dict:
            if (key == 'lat') or (key == 'lon'):
                action_str["_source"]["position"][key] = geo_obj_dict[key]
                if retain_lat_lon:
                    action_str["_source"][key] = geo_obj_dict[key]
            else:
                action_str["_source"][key] = geo_obj_dict[key]
        # pp.pprint(action_str)

        actions_list.append(action_str)

        # check if the number of bulk actions has been reached and perform a bulk doc index to db
        if len(actions_list) == n_docs_bulk:
            success, _ = helpers.bulk(es_obj, actions_list, raise_on_error=True)
            actions_list = []  # reset the list of actions to empty
            n_success += success

    # catch any remaining docs to index
    success, _ = helpers.bulk(es_obj, actions_list, raise_on_error=True)
    n_success += success

    # reset index settings after bulk import
    settings_temp = {
        "settings": {
            "index": {
                "refresh_interval": "1s",
                "number_of_replicas": 1}
        }}
    res = es_obj.indices.put_settings(body=settings_temp, index=index_name)

    return actions_list, n_success


def insert_dict_doc(data_dicts, es_obj, index_name, n_docs_bulk):
    # perform bulk inserts for any index that does not contain a lat/lon geopoint field
    # the input data should be matched with an index mapping that defines the key types for the database
    # the input data variable is 'data_dicts' this should contain keys that exactly match the mapping

    # pp = pprint.PrettyPrinter(indent=4)

    # setup the index for bulk imports
    settings_temp = {
        "settings": {
            "index": {
                "refresh_interval": -1,
                "number_of_replicas": 0}
        }}
    res = es_obj.indices.put_settings(body=settings_temp, index=index_name)

    # calculate the number of documents
    n_docs = len(data_dicts)

    # compute data attributes for the file
    actions_list = []
    n_success = 0
    for data_obj_dict in data_dicts:
        # define the database action
        action_str = {"_index": index_name,
                      "_source": {}
                      }

        # insert data into the action
        for key in data_obj_dict:
            try:
                action_str["_source"][key] = data_obj_dict[key]
            except TypeError:
                # TODO: dead code
                Logger.log.info(key)
                Logger.log.info(data_obj_dict)
                raise TypeError('Unable to index Data Dictionary key %s' % key)
        # pp.pprint(action_str)

        actions_list.append(action_str)

        # check if the number of bulk actions has been reached and perform a bulk doc index to db
        if len(actions_list) == n_docs_bulk:
            success, _ = helpers.bulk(es_obj, actions_list, raise_on_error=True)
            actions_list = []  # reset the list of actions to empty
            n_success += success

    # catch any remaining docs to index
    success, _ = helpers.bulk(es_obj, actions_list, raise_on_error=True)
    n_success += success

    # reset index settings after bulk import
    settings_temp = {
        "settings": {
            "index": {
                "refresh_interval": "1s",
                "number_of_replicas": 1}
        }}
    res = es_obj.indices.put_settings(body=settings_temp, index=index_name)

    return actions_list, n_success
