import * as functions from 'firebase-functions';
import * as admin from 'firebase-admin';

"use strict";
admin.initializeApp();

interface Dic {
    [key: string]: number
}

export const increaseFirstNewsScore = functions.firestore
    .document('clusters/{cluster_id}')
    .onCreate((snapshot, context) => {
        const data = snapshot.data();
        const db = admin.firestore();
        if (data) {
            const groupLeader = data.groupLeader;
            const accountRef = db.collection('accounts').doc(groupLeader);
            db.runTransaction(t => {
                return t.get(accountRef)
                    .then(doc => {
                        const accountData = doc.data();
                        if (accountData) {
                            const newNumber = accountData.number_of_first_news_in_group + 1;
                            t.update(accountRef, { number_of_first_news_in_group: newNumber });
                        }

                    }).catch(err => {
                        console.log('Update failure:', err);
                    });
            }).then(result => {
                console.log('Transaction success!');
            }).catch(err => {
                console.log('Transaction failure:', err);
            });
        }
    });

export const increaseDisapprovalCount = functions.firestore
    .document('reports/{report_id}')
    .onCreate((snapshot, context) => {
        const data = snapshot.data();
        const db = admin.firestore();
        if (data) {
            const disapprovedAccount = data.account;
            const accountRef = db.collection('accounts').doc(disapprovedAccount);
            db.runTransaction(t => {
                return t.get(accountRef)
                    .then(doc => {
                        const accountData = doc.data();
                        if (accountData) {
                            const newNumber = accountData.number_of_disapprovals + 1;
                            t.update(accountRef, { number_of_disapprovals: newNumber });
                        }

                    }).catch(err => {
                        console.log('Update failure:', err);
                    });
            }).then(result => {
                console.log('Transaction success!');
            }).catch(err => {
                console.log('Transaction failure:', err);
            });
        }
    });

export const increaseApprovalCount = functions.firestore
    .document('approvals/{approval_id}')
    .onCreate((snapshot, context) => {
        const data = snapshot.data();
        const db = admin.firestore();
        if (data) {
            const reportedAccount = data.account;
            const accountRef = db.collection('accounts').doc(reportedAccount);
            db.runTransaction(t => {
                return t.get(accountRef)
                    .then(doc => {
                        const accountData = doc.data();
                        if (accountData) {
                            const newNumber = accountData.number_of_approvals + 1;
                            t.update(accountRef, { number_of_approvals: newNumber });
                        }

                    }).catch(err => {
                        console.log('Update failure:', err);
                    });
            }).then(result => {
                console.log('Transaction success!');
            }).catch(err => {
                console.log('Transaction failure:', err);
            });
        }
    });

// export const increaseReportCount = functions.firestore
//     .document('reports/{report_id}')
//     .onCreate((snapshot, context) => {
//         const data = snapshot.data();
//         const db = admin.firestore();
//         if (data) {
//             const tweet_doc_id = data.tweet_doc_id;
//             const tweetRef = db.collection('accounts').doc(tweet_doc_id);
//             db.runTransaction(t => {
//                 return t.get(tweetRef)
//                     .then(doc => {
//                         const tweetData = doc.data();
//                         if (tweetData) {
//                             const newNumber = tweetData.number_of_reports + 1;
//                             t.update(tweetRef, { number_of_reports: newNumber });
//                         }

//                     }).catch(err => {
//                         console.log('Update failure:', err);
//                     });
//             }).then(result => {
//                 console.log('Transaction success!');
//             }).catch(err => {
//                 console.log('Transaction failure:', err);
//             });
//         }
//     });

export const updateAccountInfoAfterNLP = functions.firestore
    .document('tweets/{tweet_id}')
    .onUpdate((change, context) => {
        const data = change.after.data();
        const db = admin.firestore();
        if (data) {
            const account = data.username;
            const categoryOfTweet = data.category;
            const accountRef = db.collection('accounts').doc(account);

            db.runTransaction(t => {
                return t.get(accountRef)
                    .then(doc => {
                        const accountData = doc.data();
                        if (accountData) {
                            let priority = "high" as const;
                            var message = {
                                notification: {
                                    title: accountData.name,
                                    body: data.text,
                                },
                                data: {
                                    click_action: "FLUTTER_NOTIFICATION_CLICK",
                                    news_group_id: data.cluster_id,
                                },
                                android: {
                                    priority: priority
                                },
                                topic: data.cluster_id
                            };

                            admin.messaging().send(message)
                                .then((response) => {
                                    console.log('Successfully sent message:', response);
                                })
                                .catch((error) => {
                                    console.log('Error sending message:', error);
                                });

                            const categoryCount = accountData[categoryOfTweet];

                            if (categoryCount) {
                                let newData = <Dic>{
                                    [categoryOfTweet]: categoryCount + 1,
                                    number_of_total_news: accountData.number_of_total_news + 1
                                };
                                t.update(accountRef, newData);
                            }
                            else {
                                let newData = <Dic>{
                                    [categoryOfTweet]: 1,
                                    number_of_total_news: accountData.number_of_total_news + 1
                                };
                                t.set(accountRef, newData, { merge: true });
                            }
                        }
                    }).catch(err => {
                        console.log('Update failure:', err);
                    });
            }).then(result => {
                console.log('Transaction success!');
            }).catch(err => {
                console.log('Transaction failure:', err);
            });
        }
    });