/*
 * Copyright 2017 The Hyve
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.radarbase.passive.phone

import android.content.ContentResolver
import android.content.Context
import android.content.SharedPreferences
import android.database.Cursor
import android.os.Build
import android.os.Bundle
import android.provider.ContactsContract
import org.radarbase.android.data.DataCache
import org.radarbase.android.source.AbstractSourceManager
import org.radarbase.android.source.BaseSourceState
import org.radarbase.android.source.SourceStatusListener
import org.radarbase.android.util.CoroutineTaskExecutor
import org.radarbase.android.util.OfflineProcessor
import org.radarcns.kafka.ObservationKey
import org.radarcns.passive.phone.PhoneContactList
import java.util.concurrent.TimeUnit
import kotlin.collections.LinkedHashSet

class PhoneContactListManager(service: PhoneContactsListService) : AbstractSourceManager<PhoneContactsListService, BaseSourceState>(service) {
    private val preferences: SharedPreferences = service.getSharedPreferences(PhoneContactListManager::class.java.name, Context.MODE_PRIVATE)
    private val contactsTopic: DataCache<ObservationKey, PhoneContactList> = createCache(
        "android_phone_contacts",
        PhoneContactList()
    )
    private val processor: OfflineProcessor
    private val db: ContentResolver = service.contentResolver
    private var savedContactLookups: Set<String> = emptySet()

    private val contactsTaskExecutor = CoroutineTaskExecutor(this::class.simpleName!!)

    init {
        name = service.getString(R.string.contact_list)
        processor = OfflineProcessor(service) {
            process = listOf(this@PhoneContactListManager::processContacts)
            requestCode = CONTACTS_LIST_UPDATE_REQUEST_CODE
            requestName = ACTION_UPDATE_CONTACTS_LIST
            wake = false
        }
        contactsTaskExecutor.start()
    }

    override fun start(acceptableIds: Set<String>) {
        status = SourceStatusListener.Status.READY
        register()

        processor.start {
            // deprecated using contact _ID, using LOOKUP instead.
            preferences.edit()
                    .remove(CONTACT_IDS)
                    .apply()

            savedContactLookups = preferences.getStringSet(CONTACT_LOOKUPS, emptySet()) ?: emptySet()
        }

        status = SourceStatusListener.Status.CONNECTED
    }

    private fun queryContacts(): Set<String>? {
        val contactIds = LinkedHashSet<String>()
        val limit = 1000
        var where: String? = null
        var whereArgs: Array<String>? = null

        val currentIds = mutableListOf<String>()
        val sortOrder = "lookup ASC LIMIT $limit"
        val bundle = Bundle().apply {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                putInt(ContentResolver.QUERY_ARG_LIMIT, limit)
                putStringArray(ContentResolver.QUERY_ARG_SORT_COLUMNS, arrayOf("lookup"))
                putInt(ContentResolver.QUERY_ARG_SORT_DIRECTION,
                    ContentResolver.QUERY_SORT_DIRECTION_ASCENDING)
            }
        }

        do {
            currentIds.clear()

            makeQuery(bundle, sortOrder, where, whereArgs)
                ?.use { cursor ->
                    while (cursor.moveToNext()) {
                        currentIds += cursor.getString(0)
                    }
                }
                ?: return null

            if (currentIds.isNotEmpty()) {
                if (whereArgs == null) {
                    where = ContactsContract.Contacts.LOOKUP_KEY + " > ?"
                    whereArgs = arrayOf(currentIds.last())
                } else {
                    whereArgs[0] = currentIds.last()
                }
                contactIds.addAll(currentIds)
            }
        } while (currentIds.size == limit && !processor.isDone)

        return contactIds
    }

    override fun onClose() {
        contactsTaskExecutor.stop {
            processor.stop()
        }
    }

    private fun makeQuery(
        context: Bundle,
        sortOrder: String,
        where: String?,
        whereArgs: Array<String>?,
    ): Cursor? {
        return if (Build.VERSION.SDK_INT < Build.VERSION_CODES.O) {
            db.query(ContactsContract.Contacts.CONTENT_URI, LOOKUP_COLUMNS,
                where, whereArgs, sortOrder)
        } else {
            context.apply {
                putString(ContentResolver.QUERY_ARG_SQL_SELECTION, where)
                putStringArray(
                    ContentResolver.QUERY_ARG_SQL_SELECTION_ARGS,
                    whereArgs
                )
            }
            db.query(ContactsContract.Contacts.CONTENT_URI, LOOKUP_COLUMNS, context, null)
        }
    }

    private suspend fun processContacts() {
        val newContactLookups = queryContacts() ?: return

        var added: Int? = null
        var removed: Int? = null

        if (savedContactLookups.isNotEmpty()) {
            added = differenceSize(newContactLookups, savedContactLookups)
            removed = differenceSize(savedContactLookups, newContactLookups)
        }

        savedContactLookups = newContactLookups
        preferences.edit()
                .putStringSet(CONTACT_LOOKUPS, savedContactLookups)
                .apply()

        val timestamp = currentTime
        send(contactsTopic, PhoneContactList(timestamp, timestamp, added, removed, newContactLookups.size))
    }

    internal fun setCheckInterval(checkInterval: Long, unit: TimeUnit) {
        processor.interval(checkInterval, unit)
    }

    companion object {
        private const val CONTACTS_LIST_UPDATE_REQUEST_CODE = 15765692
        private const val ACTION_UPDATE_CONTACTS_LIST = "org.radarbase.passive.phone.PhoneContactListManager.ACTION_UPDATE_CONTACTS_LIST"
        private val LOOKUP_COLUMNS = arrayOf(ContactsContract.Contacts.LOOKUP_KEY)
        const val CONTACT_IDS = "contact_ids"
        const val CONTACT_LOOKUPS = "contact_lookups"

        private fun differenceSize(collectionA: Collection<*>, collectionB: Collection<*>): Int {
            return collectionA.count { it !in collectionB }
        }
    }
}
